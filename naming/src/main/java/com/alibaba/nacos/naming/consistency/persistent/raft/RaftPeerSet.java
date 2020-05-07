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
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.HttpURLConnection;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * Raft节点列表
 * @author nacos
 */
@Component
@DependsOn("serverListManager")
public class RaftPeerSet implements ServerChangeListener, ApplicationContextAware {

    /**
     * 服务列表管理器
     */
    @Autowired
    private ServerListManager serverListManager;

    private ApplicationContext applicationContext;

    /**
     * 本地选举时段
     */
    private AtomicLong localTerm = new AtomicLong(0L);

    /**
     * 当前的leader节点
     */
    private RaftPeer leader = null;

    /**
     * 节点缓存字典表
     * key: ip
     * value: Raft节点
     */
    private Map<String, RaftPeer> peers = new HashMap<>();

    private Set<String> sites = new HashSet<>();

    private boolean ready = false;

    public RaftPeerSet() {

    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        // 注册服务列表更新listener
        serverListManager.listen(this);
    }

    public RaftPeer getLeader() {
        // 如果是单机模式，返回本地节点为leader节点
        if (STANDALONE_MODE) {
            return local();
        }
        // 否则返回当前Raft集群的leader节点
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    public boolean isReady() {
        return ready;
    }

    /**
     * 移除给定服务集群列表
     * @param servers
     */
    public void remove(List<String> servers) {
        for (String server : servers) {
            // 移除服务集群
            peers.remove(server);
        }
    }

    /**
     * 更新给定的Raft节点
     * @param peer 更新的Raft节点
     * @return 更新后的Raft节点
     */
    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    /**
     * 判定给定的ip地址是否是leader节点
     * @param ip
     * @return
     */
    public boolean isLeader(String ip) {
        // 如果是单机模式，无法判定，直接返回true
        if (STANDALONE_MODE) {
            return true;
        }
        // 如果当前没有leader节点，则必定返回false
        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }
        // 判断给定的地址是否是leader节点的ip地址
        return StringUtils.equals(leader.ip, ip);
    }

    /**
     * @return 所有Raft节点的ip地址
     */
    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    /**
     * @return 除了本机ip地址的其他Raft节点的ip地址
     */
    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<>(peers.keySet());
        servers.remove(local().ip);
        return servers;
    }

    /**
     * @return 所有的Raft节点
     */
    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    /**
     * 当前Raft节点的个数
     * @return
     */
    public int size() {
        return peers.size();
    }

    /**
     * 决定候选Raft节点是否是leader节点
     * @param candidate 候选leader节点
     * @return 新leader节点
     */
    public RaftPeer decideLeader(RaftPeer candidate) {
        // 更新候选节点的节点信息
        peers.put(candidate.ip, candidate);

        SortedBag ips = new TreeBag();
        int maxApproveCount = 0;
        String maxApprovePeer = null;
        for (RaftPeer peer : peers.values()) {
            // 排除没有进行投票的
            if (StringUtils.isEmpty(peer.voteFor)) {
                continue;
            }
            // 根据投票的ip地址进行排序
            ips.add(peer.voteFor);
            // 如果选举出的ip节点的总票数超过了目前最大单节点得票数，更新最大单节点得票数
            if (ips.getCount(peer.voteFor) > maxApproveCount) {
                maxApproveCount = ips.getCount(peer.voteFor);
                // 设置最大得票数节点
                maxApprovePeer = peer.voteFor;
            }
        }

        // 如果最大得票数已经满足成为leader节点得票数的条件
        if (maxApproveCount >= majorityCount()) {
            // 获取最大得票数的Raft节点
            RaftPeer peer = peers.get(maxApprovePeer);
            // 将当前节点设置为leader节点
            peer.state = RaftPeer.State.LEADER;
            // 更新leader节点
            if (!Objects.equals(leader, peer)) {
                leader = peer;
                // 通过Spring的事件管理发布LeaderElectFinishedEvent事件
                applicationContext.publishEvent(new LeaderElectFinishedEvent(this, leader));
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }

        return leader;
    }

    /**
     * 使候选节点成为leader节点，并更新节点缓存的leader节点信息
     * @param candidate 候选leader节点
     * @return
     */
    public RaftPeer makeLeader(RaftPeer candidate) {
        // 如果leader节点发生了变更
        if (!Objects.equals(leader, candidate)) {
            // 直接将候选节点置为新的leader节点
            leader = candidate;
            // 发送新leader节点事件
            applicationContext.publishEvent(new MakeLeaderEvent(this, leader));
            Loggers.RAFT.info("{} has become the LEADER, local: {}, leader: {}",
                leader.ip, JSON.toJSONString(local()), JSON.toJSONString(leader));
        }
        // 如果当前节点认为leader节点没有发生变更，则需要更新自己存储的节点缓存中所有的节点的leader节点
        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            // 在除候选节点之外的其他节点，出现节点状态为Leader的情况下
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) {
                try {
                    // 向此节点发送请求，获取最新的leader节点信息
                    String url = RaftCore.buildURL(peer.ip, RaftCore.API_GET_PEER);
                    HttpClient.asyncHttpGet(url, null, params, new AsyncCompletionHandler<Integer>() {
                        @Override
                        public Integer onCompleted(Response response) throws Exception {
                            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                                Loggers.RAFT.error("[NACOS-RAFT] get peer failed: {}, peer: {}",
                                    response.getResponseBody(), peer.ip);
                                // 如果请求失败，直接将此节点的状态置为Follower
                                peer.state = RaftPeer.State.FOLLOWER;
                                return 1;
                            }
                            // 获取到此节点的最新信息，需要进行更新，更新方式为直接覆盖
                            update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));

                            return 0;
                        }
                    });
                } catch (Exception e) {
                    // 出现异常的情况下，直接将此节点状态置为Follower
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }
        // 更新当前节点缓存记录的候选节点的信息
        return update(candidate);
    }

    /**
     * @return 获取本地节点
     */
    public RaftPeer local() {
        // 从本地缓存中获取本地节点信息
        RaftPeer peer = peers.get(NetUtils.localServer());
        // 如果没有本地节点信息，并且使用的是单机模式，则创建一个新的本地节点
        // 因为本地缓存只能通过ServerListManager进行更新
        // 所以在第一次调用时，创建一个新的放入到本地缓存中
        if (peer == null && SystemUtils.STANDALONE_MODE) {
            RaftPeer localPeer = new RaftPeer();
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get());
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException("unable to find local peer: " + NetUtils.localServer() + ", all peers: "
                + Arrays.toString(peers.keySet().toArray()));
        }
        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    /**
     * @return 成为leader节点的最少得票数
     */
    public int majorityCount() {
        return peers.size() / 2 + 1;
    }

    public void reset() {

        leader = null;

        for (RaftPeer peer : peers.values()) {
            peer.voteFor = null;
        }
    }

    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    @Override
    public void onChangeServerList(List<Server> latestMembers) {
        Map<String, RaftPeer> tmpPeers = new HashMap<>(8);
        // 更新从集群列表管理器获取的最新的节点列表
        for (Server member : latestMembers) {
            // 如果本地节点缓存集合中存在此节点
            if (peers.containsKey(member.getKey())) {
                // 覆盖节点信息
                tmpPeers.put(member.getKey(), peers.get(member.getKey()));
                continue;
            }
            // 本地缓存没有此节点，证明是一个新加入的节点
            RaftPeer raftPeer = new RaftPeer();
            raftPeer.ip = member.getKey();

            if (NetUtils.localServer().equals(member.getKey())) {
                // 设置本地节点所在的选举时段
                raftPeer.term.set(localTerm.get());
            }
            // 放入到临时缓存中
            tmpPeers.put(member.getKey(), raftPeer);
        }

        // 更新本地缓存
        peers = tmpPeers;
        // 如果当前节点也作为一个服务暴露端口，则将当前节点状态置为准备就绪
        if (RunningConfig.getServerPort() > 0) {
            ready = true;
        }

        Loggers.RAFT.info("raft peers changed: " + latestMembers);
    }

    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

    }
}
