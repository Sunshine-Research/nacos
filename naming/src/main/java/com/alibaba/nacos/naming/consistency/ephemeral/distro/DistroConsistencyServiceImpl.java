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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 一致性协议算法，名称为<b>Distro</b>
 * <b>Distro</b>用于管理Nacos中的临时节点
 * <p>
 * distro算法将数据分割为多个数据块，每个Nacos服务节点负责仅有的一个数据块
 * 每个数据块的由其负责Nacos服务节点负责生成，移除和同步
 * 所以每个Nacos服务节点仅处理全部服务数据的一部分
 * <p>
 * 同时每个Nacos服务节点接收其他Nacos服务节点的数据同步，所以每个Nacos服务节点其实也拥有一个完全的数据
 * @author nkorange
 * @since 1.0.0
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

    /**
     * Nacos健康服务节点listener，用于判断指定service是否由当前节点进行负责
     */
    @Autowired
    private DistroMapper distroMapper;

    /**
     * 临时节点数据存储
     */
    @Autowired
    private DataStore dataStore;

    /**
     * 任务调度器
     */
    @Autowired
    private TaskDispatcher taskDispatcher;

    @Autowired
    private Serializer serializer;

    /**
     * 服务列表管理器
     */
    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private SwitchDomain switchDomain;

    /**
     * 全局配置
     */
    @Autowired
    private GlobalConfig globalConfig;
    /**
     * 是否初始化
     */
    private boolean initialized = false;
    /**
     * 任务处理器
     */
    private volatile Notifier notifier = new Notifier();
    /**
     * 数据加载任务
     */
    private LoadDataTask loadDataTask = new LoadDataTask();
    /**
     * service数据变化的listener
     */
    private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();
    /**
     * 数据更新后，需要更新checkSum，此任务用于同步checkSum
     */
    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    @PostConstruct
    public void init() {
        // 初始化时，需要进行数据加载任务
        GlobalExecutor.submit(loadDataTask);
        GlobalExecutor.submitDistroNotifyTask(notifier);
    }

    /**
     * Nacos服务节点启动时，需要进行的数据加载
     */
    private class LoadDataTask implements Runnable {

        @Override
        public void run() {
            try {
                // 加载数据
                load();
                // 如果初始化失败，则会在使用调度器，重新调度一次初始化数据加载任务
                if (!initialized) {
                    GlobalExecutor.submit(this, globalConfig.getLoadDataRetryDelayMillis());
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("load data failed.", e);
            }
        }
    }

    /**
     * 加载数据
     * @throws Exception 出现异常
     */
    public void load() throws Exception {
        // 如果是单机模式，无临时数据需要加载，直接置为初始化成功
        if (SystemUtils.STANDALONE_MODE) {
            initialized = true;
            return;
        }
        // 如果当前服务列表中仅有当前Nacos服务节点，则需要等待另一个服务节点
        while (serverListManager.getHealthyServers().size() <= 1) {
            Thread.sleep(1000L);
            Loggers.DISTRO.info("waiting server list init...");
        }
        // 遍历所有通过健康检查的Nacos服务节点
        for (Server server : serverListManager.getHealthyServers()) {
            // 跳过本地节点
            if (NetUtils.localServer().equals(server.getKey())) {
                continue;
            }
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("sync from " + server);
            }
            // 尝试从其他Nacos节点同步数据，同步成功后，初始化成功
            if (syncAllDataFromRemote(server)) {
                // 同步成功后，视为初始化成功
                initialized = true;
                return;
            }
        }
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        // 写入临时service
        onPut(key, value);
        // 触发实例变动listener的回调任务
        taskDispatcher.addTask(key);
    }

    @Override
    public void remove(String key) throws NacosException {
        // 移除service
        onRemove(key);
        // 移除service关联的listener
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * 写入新数据
     * @param key   指定service
     * @param value 数据
     */
    public void onPut(String key, Record value) {
        // 如果是临时节点，则写入内存数据存储DataStore中
        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            datum.timestamp.incrementAndGet();
            dataStore.put(key, datum);
        }
        // 如果没有指定service的listener，此时可以直接返回
        if (!listeners.containsKey(key)) {
            return;
        }
        // 有指定service的listener，则添加一个Distro算法的任务，用于触发所有listener的回调任务
        notifier.addTask(key, ApplyAction.CHANGE);
    }

    /**
     * 移除指定key
     * @param key 指定key
     */
    public void onRemove(String key) {
        // 先从内存数据存储DataSource中移除
        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }
        // 添加异步任务，触发所有listener的回调任务
        notifier.addTask(key, ApplyAction.DELETE);
    }

    /**
     * 收到更新的checksum
     * 每次数据更新之后，都会重算checksum的值
     * @param checksumMap 需要更新的checksum集合
     * @param server      更新来源节点
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        if (syncChecksumTasks.containsKey(server)) {
            // 当前异步任务中已经在处理当前server的checksum同步任务，无需继续添加
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }
        // checksum同步缓存设置当前server标志，避免重复更新
        syncChecksumTasks.put(server, "1");

        try {

            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                // 如果本地Nacos服务节点负责指定数据的key
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // 这个key不应该被其他Nacos服务节点发送过来
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // 中断处理此数据的进程
                    return;
                }
                // 如果当前本地数据存储中没有此数据，或者数据的值为null，或者数据存储的checksum不等于需要同步的checksum
                // 则当前数据需要更新，添加到更新缓存中
                if (!dataStore.contains(entry.getKey()) ||
                    dataStore.get(entry.getKey()).value == null ||
                    !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            for (String key : dataStore.keys()) {
                // 如果给定的Nacos服务节点并不负责此数据，则不进行处理
                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }
                // 移除需要移除的数据
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            }
            // 移除需要移除的数据
            for (String key : toRemoveKeys) {
                onRemove(key);
            }
            // 如果没有需要更新的，则无需进行后续处理
            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                // 从给定Nacos服务节点拉取最新的数据
                byte[] result = NamingProxy.getData(toUpdateKeys, server);
                // 并写入缓存中
                processData(result);
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // 移除this引用
            syncChecksumTasks.remove(server);
        }

    }

    /**
     * 从其他Nacos服务节点同步数据
     * @param server 其他Nacos服务节点
     * @return 同步数据是否成功
     */
    public boolean syncAllDataFromRemote(Server server) {

        try {
            // 从其他Nacos服务节点以HTTP请求的方式请求数据
            byte[] data = NamingProxy.getAllData(server.getKey());
            // 处理数据
            processData(data);
            return true;
        } catch (Exception e) {
            Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
            return false;
        }
    }

    /**
     * 处理从其他Nacos服务节点拉取的数据
     * @param data 其他Nacos服务节点拉取的数据
     * @throws Exception 出现异常
     */
    public void processData(byte[] data) throws Exception {
        if (data.length > 0) {
            // 反序列化为serviceName=>Datum<Instances>对象
            Map<String, Datum<Instances>> datumMap =
                serializer.deserializeMap(data, Instances.class);

            // 遍历所有拉取的数据
            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                // 向数据内存缓存中写入指定service和实例列表
                dataStore.put(entry.getKey(), entry.getValue());
                // 如果当前没有指定service的listener
                if (!listeners.containsKey(entry.getKey())) {
                    // 非常确定该服务不存在
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // 创建一个新的服务
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();
                        // 触发"com.alibaba.nacos.naming.domains.meta."listener的回调onChange()回调任务
                        listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0)
                            .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // 应该不会发生
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    // 触发指定service的所有listener
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // 如果触发listener成功，则更新数据缓存
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        // 如果指定service还没有listener，则为此service创建一个用于存储listener的容器
        if (!listeners.containsKey(key)) {
            listeners.put(key, new CopyOnWriteArrayList<>());
        }
        // 如果指定service已经包含了此listener，则直接返回，无需重新添加
        if (listeners.get(key).contains(listener)) {
            return;
        }
        // 向指定service的listener缓存中添加新的listener
        listeners.get(key).add(listener);
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        // 将指定的listener和指定的key进行解绑
        if (!listeners.containsKey(key)) {
            return;
        }
        // 由于listener在注册时避免了重复，删除时只需删除一个后即可返回
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        // distro一致性协议目前是否初始化完成，或者Nacos服务是否已经启动完成
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    /**
     * Distro一致性协议的任务处理任务
     */
    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);
        /**
         * 任务队列
         */
        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

        public void addTask(String datumKey, ApplyAction action) {
            // 如果当前service服务列表中已经包含了指定service，并且动作是更新操作，则不需要添加任务
            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            // 如果没有此service，则添加此service
            if (action == ApplyAction.CHANGE) {
                services.put(datumKey, StringUtils.EMPTY);
            }
            // 触发给定service的所有listener的异步任务
            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            while (true) {
                try {
                    // 从任务队列中获取任务
                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }
                    // 获取指定service名称和数据操作类型
                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();
                    // 从当前service缓存中移除指定service
                    services.remove(datumKey);

                    int count = 0;
                    // 如果当前listener集合中没有此service关联的listener，则无需触发
                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }
                    // 遍历指定service关联的所有listener
                    for (RecordListener listener : listeners.get(datumKey)) {
                        // 记录触发listener的数量
                        count++;

                        try {
                            // 根据不同的事件触发不同的回调任务
                            if (action == ApplyAction.CHANGE) {
                                listener.onChange(datumKey, dataStore.get(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                    }
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }
    }
}
