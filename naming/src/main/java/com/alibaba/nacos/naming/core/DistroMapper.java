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
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * distro算法下，用于监听Nacos服务节点变更的listener
 * 仅会执行onChangeHealthyServerList()
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper implements ServerChangeListener {

    private List<String> healthyList = new ArrayList<>();

    public List<String> getHealthyList() {
        return healthyList;
    }

    /**
     * 全局开关对象
     */
    @Autowired
    private SwitchDomain switchDomain;

    /**
     * Nacos服务节点变更管理器
     */
    @Autowired
    private ServerListManager serverListManager;

    /**
     * 监听Nacos服务节点变更
     */
    @PostConstruct
    public void init() {
        serverListManager.listen(this);
    }

    /**
     * 当前Nacos服务节点是否负责当前实例
     * @param cluster  指定集群
     * @param instance 指定实例
     * @return 当前Nacos服务节点是否负责当前实例
     */
    public boolean responsible(Cluster cluster, Instance instance) {
        // service是否开启了健康检查
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName())
            // 集群的健康检查任务没有取消
            && !cluster.getHealthCheckTask().isCancelled()
            // 当前节点是否负责当前service的数据
            && responsible(cluster.getServiceName())
            // 当前集群是否包含指定实例
            && cluster.contains(instance);
    }

    /**
     * 当前Nacos服务节点是否负责指定service
     * @param serviceName 指定service名称
     * @return 当前Nacos服务节点是否负责指定service
     */
    public boolean responsible(String serviceName) {
        // 未开启distro算法，或者是单机模式，则每个Nacos节点上都会存储所有的数据
        if (!switchDomain.isDistroEnabled() || SystemUtils.STANDALONE_MODE) {
            return true;
        }
        // 如果当前并没有健康的Nacos服务节点，则很可能是distro还没有配置完成
        if (CollectionUtils.isEmpty(healthyList)) {
            return false;
        }

        // 获取当前节点在健康服务列表中，第一次出现和最后一次出现的索引
        int index = healthyList.indexOf(NetUtils.localServer());

        int lastIndex = healthyList.lastIndexOf(NetUtils.localServer());
        if (lastIndex < 0 || index < 0) {
            return true;
        }
        // 使用distroHash算出指定service的hash值，distroHash算法实现非常简单，是serviceName的hash值的绝对值
        // 算出出serviceName的distroHash值在Nacos健康集群中的索引
        int target = distroHash(serviceName) % healthyList.size();
        // 判断索引是否在当前Nacos服务节点负责范围内
        return target >= index && target <= lastIndex;
    }

    /**
     * 获取负责指定service的Nacos服务节点名称
     * @param serviceName 指定service名称
     * @return 负责指定service的Nacos服务节点名称
     */
    public String mapSrv(String serviceName) {
        // 如果当前健康列表为空，或者未开启distro，则返回本地Nacos服务节点
        if (CollectionUtils.isEmpty(healthyList) || !switchDomain.isDistroEnabled()) {
            return NetUtils.localServer();
        }

        try {
            // 获取负责给定serviceName的Nacos服务节点名称
            return healthyList.get(distroHash(serviceName) % healthyList.size());
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);

            return NetUtils.localServer();
        }
    }

    /**
     * distro算法
     * @param serviceName 指定service名称
     * @return distro算法下的hash值
     */
    public int distroHash(String serviceName) {
        // serviceName的hash值
        return Math.abs(serviceName.hashCode() % Integer.MAX_VALUE);
    }

    @Override
    public void onChangeServerList(List<Server> latestMembers) {

    }

    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {
        // 遍历最进可访问的Nacos成员列表，添加到健康Nacos节点的本地缓存中
        List<String> newHealthyList = new ArrayList<>();
        for (Server server : latestReachableMembers) {
            newHealthyList.add(server.getKey());
        }
        healthyList = newHealthyList;
    }
}
