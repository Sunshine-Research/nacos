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
package com.alibaba.nacos.naming.healthcheck;


import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 客户端发起的心跳请求，用于处理临时节点的心跳请求任务
 * @author nkorange
 */
public class ClientBeatProcessor implements Runnable {
    public static final long CLIENT_BEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);
    private RsInfo rsInfo;
    private Service service;

    @JSONField(serialize = false)
    public PushService getPushService() {
        return SpringContext.getAppContext().getBean(PushService.class);
    }

    public RsInfo getRsInfo() {
        return rsInfo;
    }

    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public void run() {
        Service service = this.service;
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }
        // 获取ip，port，clusterName
        String ip = rsInfo.getIp();
        String clusterName = rsInfo.getCluster();
        int port = rsInfo.getPort();
        Cluster cluster = service.getClusterMap().get(clusterName);
        // 获取指定service，指定cluster下的所有临时节点
        List<Instance> instances = cluster.allIPs(true);
        // 遍历所有的临时节点
        for (Instance instance : instances) {
            // 如果找到了心跳上报的客户端节点
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                // 设置实例的最新心跳信息
                instance.setLastBeat(System.currentTimeMillis());
                // 如果节点没有被标记，并且处于非健康状态，则更新节点状态为健康状态
                if (!instance.isMarked()) {
                    if (!instance.isHealthy()) {
                        instance.setHealthy(true);
                        Loggers.EVT_LOG.info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                            cluster.getService().getName(), ip, port, cluster.getName(), UtilsAndCommons.LOCALHOST_SITE);
                        getPushService().serviceChanged(service);
                    }
                }
            }
        }
    }
}
