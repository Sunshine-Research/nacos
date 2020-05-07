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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;

import java.net.HttpURLConnection;
import java.util.List;


/**
 * 检查和更新临时实例的状态，如果实例节点状态已经过期，则会进行移除
 * @author nkorange
 */
public class ClientBeatCheckTask implements Runnable {

    private Service service;

    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }


    @JSONField(serialize = false)
    public PushService getPushService() {
        return SpringContext.getAppContext().getBean(PushService.class);
    }

    @JSONField(serialize = false)
    public DistroMapper getDistroMapper() {
        return SpringContext.getAppContext().getBean(DistroMapper.class);
    }

    public GlobalConfig getGlobalConfig() {
        return SpringContext.getAppContext().getBean(GlobalConfig.class);
    }

    public SwitchDomain getSwitchDomain() {
        return SpringContext.getAppContext().getBean(SwitchDomain.class);
    }

    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName());
    }

    @Override
    public void run() {
        try {
            // 指定service不是当前Nacos服务节点负责，则不进行任务处理
            if (!getDistroMapper().responsible(service.getName())) {
                return;
            }
            // 如果关闭了健康检查功能，也不进行任务处理
            if (!getSwitchDomain().isHealthCheckEnabled()) {
                return;
            }
            // 获取当前service下，所有的实例信息，包括属于集群的部分
            List<Instance> instances = service.allIPs(true);

            // 首先设置所有实例节点的健康状态
            for (Instance instance : instances) {
                // 如果心跳响应已经超过了要求的心跳时间间隔
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    if (!instance.isMarked()) {
                        if (instance.isHealthy()) {
                            // 将当前节点的健康状态置为false
                            instance.setHealthy(false);
                            Loggers.EVT_LOG.info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                instance.getIp(), instance.getPort(), instance.getClusterName(), service.getName(),
                                UtilsAndCommons.LOCALHOST_SITE, instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            // 发送给定service变动和实例变动事件
                            getPushService().serviceChanged(service);
                            SpringContext.getAppContext().publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
                        }
                    }
                }
            }

            // 如果全局设置无需关心过期节点，则不对过期节点进行处理
            if (!getGlobalConfig().isExpireInstance()) {
                return;
            }

            // 移除过期节点
            for (Instance instance : instances) {

                if (instance.isMarked()) {
                    continue;
                }

                // 如果心跳时间间隔已经超出了可以移除的等待时间
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(), JSON.toJSONString(instance));
                    // 移除此实例
                    deleteIP(instance);
                }
            }

        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }

    }

    /**
     * 移除实例
     * @param instance 需要移除的实例
     */
    private void deleteIP(Instance instance) {

        try {
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp())
                .appendParam("port", String.valueOf(instance.getPort()))
                .appendParam("ephemeral", "true")
                .appendParam("clusterName", instance.getClusterName())
                .appendParam("serviceName", service.getName())
                .appendParam("namespaceId", service.getNamespaceId());
            // 发送请求给当前Nacos节点，以统一入口的方式，删除实例信息
            String url = "http://127.0.0.1:" + RunningConfig.getServerPort() + RunningConfig.getContextPath()
                + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

            // 异步移除实例
            HttpClient.asyncHttpDelete(url, null, null, new AsyncCompletionHandler() {
                @Override
                public Object onCompleted(Response response) throws Exception {
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                            instance.toJSON(), response.getResponseBody(), response.getStatusCode());
                    }
                    return null;
                }
            });

        } catch (Exception e) {
            Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJSON(), e);
        }
    }
}
