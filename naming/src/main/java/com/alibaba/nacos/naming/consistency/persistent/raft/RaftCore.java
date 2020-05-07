/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * Raft选举的核心
 * @author nacos
 */
@Component
public class RaftCore {

    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    /**
     * 一个线程调度器，用于通知选举结果
     */
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.raft.notifier");

            return t;
        }
    });

    /**
     *
     */
    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;
    /**
     * 监听者listener列表
     * key: 数据key
     * value: 监听者列表
     */
    private volatile Map<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    @Autowired
    private RaftPeerSet peers;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private RaftProxy raftProxy;

    @Autowired
    private RaftStore raftStore;

    public volatile Notifier notifier = new Notifier();

    private boolean initialized = false;

    @PostConstruct
    public void init() throws Exception {

        Loggers.RAFT.info("initializing Raft sub-system");
        // 执行通知器中所有的任务
        executor.submit(notifier);

        long start = System.currentTimeMillis();
        // 加载
        raftStore.loadDatums(notifier, datums);
        // 设置选举时段，如果是刚启动的节点，则选举时段为0
        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

        while (true) {
            if (notifier.tasks.size() <= 0) {
                break;
            }
            Thread.sleep(1000L);
        }

        initialized = true;

        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));
        // 调度选举任务，每500ms调度一次
        GlobalExecutor.registerMasterElection(new MasterElection());
        // 调度心跳任务，每500ms调度一次
        GlobalExecutor.registerHeartbeat(new HeartBeat());

        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
            GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, List<RecordListener>> getListeners() {
        return listeners;
    }

    /**
     * 发布新数据
     * @param key   数据key
     * @param value 数据
     * @throws Exception
     */
    public void signalPublish(String key, Record value) throws Exception {
        // 如果当前节点不是leader
        if (!isLeader()) {
            JSONObject params = new JSONObject();
            params.put("key", key);
            params.put("value", value);
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);
            // 将此请求转发给leader节点
            raftProxy.proxyPostLarge(getLeader().ip, API_PUB, params.toJSONString(), parameters);
            return;
        }

        try {
            // 当前节点是leader节点，需要进行持久化，则先进行同步操作
            OPERATE_LOCK.lock();
            long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            if (getDatum(key) == null) {
                // 如果是新数据，则设置数据的版本号
                datum.timestamp.set(1L);
            } else {
                // 如果需要更新数据，则对递增数据的版本号
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }

            JSONObject json = new JSONObject();
            json.put("datum", datum);
            json.put("source", peers.local());
            // 发布数据，将数据落地
            onPublish(datum, peers.local());

            final String content = JSON.toJSONString(json);
            // 只需要向超过½节点数量发布数据即可
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            for (final String server : peers.allServersIncludeMyself()) {
                if (isLeader(server)) {
                    // leader节点也属于½+1的节点
                    latch.countDown();
                    continue;
                }
                // 构建发布数据请求
                final String url = buildURL(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key=" + key), content, new AsyncCompletionHandler<Integer>() {
                    @Override
                    public Integer onCompleted(Response response) throws Exception {
                        if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                            Loggers.RAFT.warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                datum.key, server, response.getStatusCode());
                            return 1;
                        }
                        // 每次成功响应，都会countDown()一下
                        latch.countDown();
                        return 0;
                    }

                    @Override
                    public STATE onContentWriteCompleted() {
                        return STATE.CONTINUE;
                    }
                });

            }
            // 在等待时间内范围内等待数据发布的响应成功
            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // 只要超过½+1数量的服务节点返回成功，我们就可以任务更新成功
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }
            // 记录数据发布的时间
            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            // 解除同步操作
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * 发布删除
     * @param key 删库数据的key
     * @throws Exception
     */
    public void signalDelete(final String key) throws Exception {
        // 需要进行同步操作
        OPERATE_LOCK.lock();
        try {
            // 仅有leader节点可以落地删除操作，如果当前节点不是leader节点，则将请求路由到leader节点
            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            JSONObject json = new JSONObject();
            // 构建删除数据请求
            Datum datum = new Datum();
            datum.key = key;
            json.put("datum", datum);
            json.put("source", peers.local());
            // 删除数据
            onDelete(datum.key, peers.local());
            // 遍历出leader节点之外的其他节点
            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildURL(server, API_ON_DEL);
                // 向每个follower节点发送数据删除请求
                HttpClient.asyncHttpDeleteLarge(url, null, JSON.toJSONString(json)
                    , new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}", key, server, response.getStatusCode());
                                return 1;
                            }

                            RaftPeer local = peers.local();
                            // 重置节点发起选举的剩余时间
                            local.resetLeaderDue();

                            return 0;
                        }
                    });
            }
        } finally {
            // 解除同步操作
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * 发布数据
     * @param datum  需要发布的数据
     * @param source 数据发布的来源节点
     * @throws Exception
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        // 获取本地节点
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }

        // 不允许非leader节点发布数据
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " +
                "data but wasn't leader");
        }
        // 如果数据来源节点所在的选举时段＜本地节点所在的选举时段，则数据发布已过期
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term.get() + ", cur-term: " + local.term.get());
        }
        // 重置当前节点的发起选举的剩余时间
        local.resetLeaderDue();

        // 如果不是临时数据，则需要进行持久化
        if (KeyBuilder.matchPersistentKey(datum.key)) {
            raftStore.write(datum);
        }
        // 将数据放入到缓存中
        datums.put(datum.key, datum);
        // 如果当前节点是leader节点，则将节点所在的选举时段递增100
        if (isLeader()) {
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            // 当前节点不是leader节点，如果本地节点+数据发布所需要跨越的选举时段数 > leader节点所在的选举时段数
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                // 更新本地节点所存储的leader节点所在的选举时段
                getLeader().term.set(source.term.get());
                // 更新本地节点所在的选举时段数为leader节点所在的选举时段
                local.term.set(getLeader().term.get());
            } else {
                // 将本地节点的选举时段递增100
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        // 由于更新了数据，先对元数据进行更新
        raftStore.updateTerm(local.term.get());
        // 添加数据更新任务，用于执行任务的回调任务
        notifier.addTask(datum.key, ApplyAction.CHANGE);

        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    /**
     * 数据删除
     * @param datumKey 删除数据的key
     * @param source   发起数据删除的节点信息
     * @throws Exception
     */
    public void onDelete(String datumKey, RaftPeer source) throws Exception {
        // 获取本地节点
        RaftPeer local = peers.local();
        // 如果本地节点认为，发起删除数据请求的节点不是leader节点，则不允许其进行数据删除
        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}",
                JSON.toJSONString(source), JSON.toJSONString(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }
        // 如果发起数据删除的节点所在的选举时段小于本地节点的选举时段，则证明删除的数据请求已经过期
        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}",
                JSON.toJSONString(source), JSON.toJSONString(local));
            throw new IllegalStateException("out of date publish, pub-term:"
                + source.term + ", cur-term: " + local.term);
        }
        // 重置发起选举的剩余时间
        local.resetLeaderDue();

        String key = datumKey;
        // 删除本地数据
        deleteDatum(key);
        // 如果是service类型的数据
        if (KeyBuilder.matchServiceMetaKey(key)) {
            // 如果本地节点+删除数据需要递增的选举时段数100 ＞ 发起删除节点所在选举时段
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                // 设置本地节点所存储的leader节点选举时段为发起数据删除节点所在的选举时段
                getLeader().term.set(source.term.get());
                // 设置本地节点的选举时段为leader节点的选举时段
                local.term.set(getLeader().term.get());
            } else {
                // 当前节点是leader节点，递增leader节点的选举时段100
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
            // 落地选举时段元数据
            raftStore.updateTerm(local.term.get());
        }

        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

    }

    /**
     * 执行选举任务
     */
    public class MasterElection implements Runnable {
        @Override
        public void run() {
            try {

                if (!peers.isReady()) {
                    return;
                }
                // 获取当前机器的Raft节点
                RaftPeer local = peers.local();
                // 触发选举条件的时间间隔
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                if (local.leaderDueMs > 0) {
                    return;
                }

                // 重置选举间隔
                local.resetLeaderDue();
                // 重置心跳响应间隔
                local.resetHeartbeatDue();
                // 发送选举投票
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }

        }

        /**
         * 发送选举投票
         */
        public void sendVote() {
            // 获取当前机器的Raft节点
            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}",
                JSON.toJSONString(getLeader()), local.term);
            // 重置所有Raft节点的投票
            peers.reset();
            // 递增当前Raft节点的选举时段
            local.term.incrementAndGet();
            // 选择自己作为新的leader节点
            local.voteFor = local.ip;
            // 将当前节点的状态置为候选状态
            local.state = RaftPeer.State.CANDIDATE;

            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JSON.toJSONString(local));
            // 遍历当前除自己外的其他Raft节点
            for (final String server : peers.allServersWithoutMySelf()) {
                // 构建投票请求
                final String url = buildURL(server, API_VOTE);
                try {
                    // 异步发送投票请求
                    HttpClient.asyncHttpPost(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            // 如果投票失败，记录日志
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", response.getResponseBody(), url);
                                return 1;
                            }
                            // 发送投票请求的节点的信息
                            RaftPeer peer = JSON.parseObject(response.getResponseBody(), RaftPeer.class);

                            Loggers.RAFT.info("received approve from peer: {}", JSON.toJSONString(peer));
                            // 记录投票信息
                            peers.decideLeader(peer);

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        // 如果当前节点记录的服务器集群中，没有包含选票中的服务节点，则直接抛出异常
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }
        // 获取当前节点信息
        RaftPeer local = peers.get(NetUtils.localServer());
        // 如果发起投票的节点所处的选举时段，小于等于当前节点所处的选举时段，证明发起的选举是无效的
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" +
                ", voter-term:" + remote.term + ", votee-term:" + local.term;
            // 返回给发起投票节点的信息为：您发起的投票请求无效，你看我比你所处的选举时段要大，要发起也是我发起
            // 返回的是当前节点，选举自己为leader节点的节点信息
            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;
            }

            return local;
        }
        // 如果发起的选举满足选举条件
        // 重置发起选举的剩余时间
        local.resetLeaderDue();
        // 当前节点置为Follower节点，
        local.state = RaftPeer.State.FOLLOWER;
        // 同意发出选举的节点成为新leader节点
        local.voteFor = remote.ip;
        // 更新当前节点所处的选举时段
        local.term.set(remote.term.get());

        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        return local;
    }

    /**
     * 心跳任务
     */
    public class HeartBeat implements Runnable {
        @Override
        public void run() {
            try {

                if (!peers.isReady()) {
                    return;
                }
                // 获取本地节点信息
                RaftPeer local = peers.local();
                // 计算发送心跳的剩余时间
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                if (local.heartbeatDueMs > 0) {
                    return;
                }
                // 重置下一次进行心跳的时间
                local.resetHeartbeatDue();
                // 发送心跳请求
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }

        }

        /**
         * 发送心跳请求
         * @throws IOException
         * @throws InterruptedException
         */
        public void sendBeat() throws IOException, InterruptedException {
            // 获取本地节点信息
            RaftPeer local = peers.local();
            // 如果在集群模式下，当前节点状态不是leader节点
            if (local.state != RaftPeer.State.LEADER && !STANDALONE_MODE) {
                return;
            }

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }
            // 重置当前节点重新发起选举的剩余时间
            local.resetLeaderDue();

            // 开始构建心跳数据请求，心跳数据存储了所有的service和instance的数据
            JSONObject packet = new JSONObject();
            packet.put("peer", local);

            JSONArray array = new JSONArray();

            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", String.valueOf(switchDomain.isSendBeatOnly()));
            }

            if (!switchDomain.isSendBeatOnly()) {
                // 组装所有的数据，数据来源于本地缓存
                for (Datum datum : datums.values()) {

                    JSONObject element = new JSONObject();

                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp);

                    array.add(element);
                }
            }

            packet.put("datums", array);
            // 接下来进行心跳请求广播
            Map<String, String> params = new HashMap<String, String>(1);
            params.put("beat", JSON.toJSONString(packet));

            String content = JSON.toJSONString(params);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}",
                    content.length(), compressedContent.length());
            }
            // 遍历所有除leader节点之外的节点
            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    // 构建心跳请求
                    final String url = buildURL(server, API_BEAT);
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}",
                                    response.getResponseBody(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return 1;
                            }
                            // 更新返回的最新的节点数据
                            peers.update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                            return 0;
                        }

                        @Override
                        public void onThrowable(Throwable t) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server, t);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }

        }
    }

    /**
     * 处理来自leader节点的心跳响应
     * @param beat 心跳响应内容
     * @return 当前节点信息
     * @throws Exception
     */
    public RaftPeer receivedBeat(JSONObject beat) throws Exception {
        final RaftPeer local = peers.local();
        final RaftPeer remote = new RaftPeer();
        // 获取心跳请求的来源ip
        remote.ip = beat.getJSONObject("peer").getString("ip");
        // 获取心跳请求的节点状态
        remote.state = RaftPeer.State.valueOf(beat.getJSONObject("peer").getString("state"));
        // 获取发起心跳请求节点所处的选举时段
        remote.term.set(beat.getJSONObject("peer").getLongValue("term"));
        // 获取发起心跳请求的时间间隔
        remote.heartbeatDueMs = beat.getJSONObject("peer").getLongValue("heartbeatDueMs");
        // 获取发起心跳请求的发起leader选举的剩余时间
        remote.leaderDueMs = beat.getJSONObject("peer").getLongValue("leaderDueMs");
        // 获取发起心跳请求的节点的选票
        remote.voteFor = beat.getJSONObject("peer").getString("voteFor");
        // 如果发起心跳请求的节点不是leader节点，则是不允许出现的行为
        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}",
                remote.state, JSON.toJSONString(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }
        // 如果当前节点所处的选举时段大于发起心跳请求的leader节点所处的选举时段，则证明心跳请求已失效
        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT.info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}"
                , remote.term.get(), local.term.get(), JSON.toJSONString(remote), local.leaderDueMs);
            throw new IllegalArgumentException("out of date beat, beat-from-term: " + remote.term.get()
                + ", beat-to-term: " + local.term.get());
        }
        // 此时证明心跳请求是有效的，如果当前节点状态不是Follower，可能是已处于candidate，也可能是发生脑裂处于leader
        if (local.state != RaftPeer.State.FOLLOWER) {
            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JSON.toJSONString(remote));
            // 将当前节点状态重新置为Follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }
        // leader节点发送的数据
        final JSONArray beatDatums = beat.getJSONArray("datums");
        // 重置当前节点发起leader节点的剩余时间和心跳间隔失效时间
        local.resetLeaderDue();
        local.resetHeartbeatDue();
        // 重置leader节点信息
        peers.makeLeader(remote);

        // 需要更新来自leader节点数据
        if (!switchDomain.isSendBeatOnly()) {

            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());

            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0);
            }

            // 进行节点数据检查
            List<String> batch = new ArrayList<>();

            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                    beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            // 遍历leader节点发送的数据
            for (Object object : beatDatums) {
                // 处理的数量
                processedCount = processedCount + 1;

                JSONObject entry = (JSONObject) object;
                String key = entry.getString("key");
                final String datumKey;
                if (KeyBuilder.matchServiceMetaKey(key)) {
                    // 如果是service数据
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    // 如果是instance数据
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // 其他数据不进行处理
                    continue;
                }

                long timestamp = entry.getLong("timestamp");

                receivedKeysMap.put(datumKey, 1);

                try {
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp && processedCount < beatDatums.size()) {
                        // 收到的数据在本地存在，但是本地的版本≥当前的版本，并且当前处理的数量小于接收到的数据的数量
                        // 不处理当前数据，继续下一个数据的处理
                        continue;
                    }

                    // 如果本地数据缓存中不存在当前数据，或者本地数据的版本小于当前数据的版本
                    // 放入到更新缓存中
                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        batch.add(datumKey);
                    }

                    // 如果需要更新数据数量没有超过50个，并且处理数目没有超过数据处理的流程
                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }
                    // 将所有需要更新的数据名称使用","分隔成为一个名称总和
                    String keys = StringUtils.join(batch, ",");
                    // 如果没有数据需要进行拉取，则不进行拉取
                    if (batch.size() <= 0) {
                        continue;
                    }

                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}, datums' size is {}, RaftCore.datums' size is {}"
                        , getLeader().ip, batch.size(), processedCount, beatDatums.size(), datums.size());

                    // 构建HTTP请求，从leader节点拉取数据
                    String url = buildURL(remote.ip, API_GET) + "?keys=" + URLEncoder.encode(keys, "UTF-8");
                    HttpClient.asyncHttpGet(url, null, null, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                return 1;
                            }
                            // 由于是批量拉取，所以会返回一个列表
                            List<JSONObject> datumList = JSON.parseObject(response.getResponseBody(), new TypeReference<List<JSONObject>>() {
                            });
                            // 遍历每个
                            for (JSONObject datumJson : datumList) {
                                // 进行同步操作
                                OPERATE_LOCK.lock();
                                Datum newDatum = null;
                                try {
                                    // 获取当前缓存中的数据
                                    Datum oldDatum = getDatum(datumJson.getString("key"));
                                    // 如果缓存数据版本号≥新拉取数据的版本号，则不进行数据的更新
                                    if (oldDatum != null && datumJson.getLongValue("timestamp") <= oldDatum.timestamp.get()) {
                                        Loggers.RAFT.info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                            datumJson.getString("key"), datumJson.getLongValue("timestamp"), oldDatum.timestamp);
                                        continue;
                                    }
                                    // 如果是service数据，更新service数据
                                    if (KeyBuilder.matchServiceMetaKey(datumJson.getString("key"))) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.getString("key");
                                        serviceDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        serviceDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Service.class);
                                        newDatum = serviceDatum;
                                    }
                                    // 如果是instance数据，更新instance数据
                                    if (KeyBuilder.matchInstanceListKey(datumJson.getString("key"))) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.getString("key");
                                        instancesDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                                        instancesDatum.value =
                                            JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")), Instances.class);
                                        newDatum = instancesDatum;
                                    }
                                    // 接收到的为null数据，不进行落地
                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }
                                    // 使用RaftScore对象进行落地
                                    raftStore.write(newDatum);
                                    // 先更新缓存
                                    datums.put(newDatum.key, newDatum);
                                    notifier.addTask(newDatum.key, ApplyAction.CHANGE);
                                    // 此时也算是与leader节点进行了通信，也需要重置leader选举的剩余时间
                                    local.resetLeaderDue();

                                    if (local.term.get() + 100 > remote.term.get()) {
                                        // 更新本地节点缓存信息中的leader节点的选举时段为远程节点的选举时段
                                        getLeader().term.set(remote.term.get());
                                        // 更新本地节点选举时段为leader节点的选举时段
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        // 否则，将本地节点的选举时段递增100
                                        local.term.addAndGet(100);
                                    }
                                    // 更新选举时段
                                    raftStore.updateTerm(local.term.get());

                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                        newDatum.key, newDatum.timestamp, JSON.toJSONString(remote), local.term);

                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum, e);
                                } finally {
                                    // 解锁
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            // 拉取完成之后，休息0.2s
                            TimeUnit.MILLISECONDS.sleep(200);
                            return 0;
                        }
                    });
                    // 清除已经更新的数据
                    batch.clear();

                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }

            }

            // 剩余的数据即为删除的数据
            List<String> deadKeys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }
            // 从本地文件中删除数据
            for (String deadKey : deadKeys) {
                try {
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }

        }
        // 最后返回当前节点信息
        return local;
    }

    /**
     * 对指定数据key添加回调任务
     * @param key      需要添加回调任务的数据key
     * @param listener 添加的回调任务
     */
    public void listen(String key, RecordListener listener) {
        // 从回调任务缓存中获取当前数据key的回调任务
        List<RecordListener> listenerList = listeners.get(key);
        // 如果指定数据key已经包含了此回调任务，则不会添加
        if (listenerList != null && listenerList.contains(listener)) {
            return;
        }

        // 指定数据key还没有回调任务，则创建一个CopyOnWriteArrayList用于存储给定的回调任务
        if (listenerList == null) {
            listenerList = new CopyOnWriteArrayList<>();
            listeners.put(key, listenerList);
        }

        Loggers.RAFT.info("add listener: {}", key);
        // 已有指定key的回调任务列表，直接向列表中追加
        listenerList.add(listener);

        // 添加回调任务后触发一次回调任务的执行
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    /**
     * 解绑指定key的回调任务
     * @param key      需要解绑回调任务的数据key
     * @param listener 解绑的回调任务
     */
    public void unlisten(String key, RecordListener listener) {
        // 如果没有指定key的回调任务列表，直接返回
        if (!listeners.containsKey(key)) {
            return;
        }
        // 遍历指定key的回调任务列表，并判断是否为给定回调任务
        for (RecordListener dl : listeners.get(key)) {
            // TODO maybe use equal:
            if (dl == listener) {
                // 移除给定的回调任务
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    /**
     * 解绑指定数据key的所有回调任务
     * @param key 需要解绑所有任务的数据key
     */
    public void unlistenAll(String key) {
        listeners.remove(key);
    }

    /**
     * 设置本地节点的选举时段
     * @param term 本地节点的选举时段
     */
    public void setTerm(long term) {
        peers.setTerm(term);
    }

    /**
     * 指定ip地址是否是当前集群的leader节点
     * @param ip 判断是否为leader节点的ip地址
     * @return ip地址是否是当前集群的leader节点
     */
    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    /**
     * @return 判断本地节点是否是leader节点
     */
    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    /**
     * 构建HTTP请求URL
     * @param ip  请求的IP地址
     * @param api 请求方法
     * @return HTTP请求URL
     */
    public static String buildURL(String ip, String api) {
        // 组装ip:port
        if (!ip.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
            ip = ip + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
        }
        return "http://" + ip + RunningConfig.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    /**
     * @return 集群拥有的数据大小
     */
    public int datumSize() {
        return datums.size();
    }

    /**
     * 添加数据
     * @param datum 需要添加的数据
     */
    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        notifier.addTask(datum.key, ApplyAction.CHANGE);
    }

    /**
     * 加载数据
     * @param key 需要从本地加载到内存的数据
     */
    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }

    /**
     * 删除指定的数据
     * @param key 数据名称
     */
    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            // 触发数据删除的回调任务
            notifier.addTask(URLDecoder.decode(key, "UTF-8"), ApplyAction.DELETE);
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    /**
     * @return 当前节点是否初始化完成
     */
    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    /**
     * @return 需要处理的任务大小
     */
    public int getNotifyTaskCount() {
        return notifier.getTaskSize();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        /**
         * 需要处理的任务
         */
        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<>(1024 * 1024);

        /**
         * 添加任务
         * @param datumKey 服务名
         * @param action   动作类型
         */
        public void addTask(String datumKey, ApplyAction action) {
            // 如果当前服务列表中，包含了指定的服务，并且service动作类型是数据变更
            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                // 直接返回
                return;
            }

            if (action == ApplyAction.CHANGE) {
                // 此时证明服务不在当前service缓存中，添加新的service
                services.put(datumKey, StringUtils.EMPTY);
            }

            Loggers.RAFT.info("add task {}", datumKey);
            // 此时添加新的服务动作，情况可能是service缓存中没有指定的服务，或者动作类型不是数据变更
            tasks.add(Pair.with(datumKey, action));
        }

        /**
         * @return 当前任务列表大小
         */
        public int getTaskSize() {
            return tasks.size();
        }

        /**
         * 通知器的任务执行
         */
        @Override
        public void run() {
            Loggers.RAFT.info("raft notifier started");

            while (true) {
                try {
                    // 从任务队列中获取任务
                    Pair pair = tasks.take();
                    // 没有任务的话，继续自旋执行
                    if (pair == null) {
                        continue;
                    }
                    // 获取service名称
                    String datumKey = (String) pair.getValue0();
                    // 获取service的动作类型
                    ApplyAction action = (ApplyAction) pair.getValue1();
                    // 先从当前service的缓存中，移除当前service
                    services.remove(datumKey);

                    Loggers.RAFT.info("remove task {}", datumKey);

                    int count = 0;
                    // 如果需要通知"com.alibaba.nacos.naming.domains.meta."类型的监听者
                    if (listeners.containsKey(KeyBuilder.SERVICE_META_KEY_PREFIX)) {
                        // 并且当前listener的名称满足"com.alibaba.nacos.naming.domains.meta."和"meta."前缀
                        // 并且当前listener的名称不以"00-00---000-NACOS_SWITCH_DOMAIN-000---00-00"作为结尾
                        if (KeyBuilder.matchServiceMetaKey(datumKey) && !KeyBuilder.matchSwitchKey(datumKey)) {
                            // 则需要对此类型的listener进行通知
                            // 迭代"com.alibaba.nacos.naming.domains.meta."此类型下的所有listener
                            for (RecordListener listener : listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX)) {
                                try {
                                    // 数据更新和数据删除时间分开进行通知
                                    if (action == ApplyAction.CHANGE) {
                                        listener.onChange(datumKey, getDatum(datumKey).value);
                                    }

                                    if (action == ApplyAction.DELETE) {
                                        listener.onDelete(datumKey);
                                    }
                                } catch (Throwable e) {
                                    Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                                }
                            }
                        }
                    }
                    // 如果当前listener集合中，没有当前service，则继续进行
                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }
                    // 当前listener集合中，存在当前service，则对service的listener集合进行通知
                    for (RecordListener listener : listeners.get(datumKey)) {
                        // 递增通知计数器
                        count++;

                        try {
                            // 只有一个动作类型，所以通知完成即可进行下一个任务的处理
                            if (action == ApplyAction.CHANGE) {
                                listener.onChange(datumKey, getDatum(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("[NACOS-RAFT] datum change notified, key: {}, listener count: {}", datumKey, count);
                    }
                } catch (Throwable e) {
                    Loggers.RAFT.error("[NACOS-RAFT] Error while handling notifying task", e);
                }
            }
        }
    }
}
