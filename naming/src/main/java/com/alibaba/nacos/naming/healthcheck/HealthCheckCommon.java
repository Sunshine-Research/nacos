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
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Health check public methods
 * @author nkorange
 * @since 1.0.0
 */
@Component
public class HealthCheckCommon {

    @Autowired
    private DistroMapper distroMapper;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private PushService pushService;

    private static LinkedBlockingDeque<HealthCheckResult> healthCheckResults = new LinkedBlockingDeque<>(1024 * 128);

    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.taobao.health-check.notifier");
            return thread;
        }
    });


    public void init() {
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                List list = Arrays.asList(healthCheckResults.toArray());
                healthCheckResults.clear();

                List<Server> sameSiteServers = serverListManager.getServers();

                if (sameSiteServers == null || sameSiteServers.size() <= 0) {
                    return;
                }

                for (Server server : sameSiteServers) {
                    if (server.getKey().equals(NetUtils.localServer())) {
                        continue;
                    }
                    Map<String, String> params = new HashMap<>(10);
                    params.put("result", JSON.toJSONString(list));
                    if (Loggers.SRV_LOG.isDebugEnabled()) {
                        Loggers.SRV_LOG.debug("[HEALTH-SYNC] server: {}, healthCheckResults: {}",
                            server, JSON.toJSONString(list));
                    }

                    HttpClient.HttpResult httpResult = HttpClient.httpPost("http://" + server.getKey()
                        + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT
                        + "/api/healthCheckResult", null, params);

                    if (httpResult.code != HttpURLConnection.HTTP_OK) {
                        Loggers.EVT_LOG.warn("[HEALTH-CHECK-SYNC] failed to send result to {}, result: {}",
                            server, JSON.toJSONString(list));
                    }

                }

            }
        }, 500, TimeUnit.MILLISECONDS);
    }

    public void reEvaluateCheckRT(long checkRT, HealthCheckTask task, SwitchDomain.HealthParams params) {
        task.setCheckRTLast(checkRT);

        if (checkRT > task.getCheckRTWorst()) {
            task.setCheckRTWorst(checkRT);
        }

        if (checkRT < task.getCheckRTBest()) {
            task.setCheckRTBest(checkRT);
        }

        checkRT = (long) ((params.getFactor() * task.getCheckRTNormalized()) + (1 - params.getFactor()) * checkRT);

        if (checkRT > params.getMax()) {
            checkRT = params.getMax();
        }

        if (checkRT < params.getMin()) {
            checkRT = params.getMin();
        }

        task.setCheckRTNormalized(checkRT);
    }

    /**
     * 实例通过健康检查
     * @param ip   通过健康检查的实例
     * @param task 健康检查任务
     * @param msg  健康检查信息
     */
    public void checkOK(Instance ip, HealthCheckTask task, String msg) {
        Cluster cluster = task.getCluster();

        try {
            // 如果实例并不处于健康状态，或者实例mock为不合法的
            if (!ip.isHealthy() || !ip.isMockValid()) {
                // 如果实例返回的OK次数，不小于当前进行检查的次数
                if (ip.getOKCount().incrementAndGet() >= switchDomain.getCheckTimes()) {
                    // 并且当前Nacos服务节点负责此实例
                    if (distroMapper.responsible(cluster, ip)) {
                        // 实例设置为健康状态
                        ip.setHealthy(true);
                        ip.setMockValid(true);
                        // 更新service信息
                        Service service = cluster.getService();
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        // 发布service变更事件
                        pushService.serviceChanged(service);
                        // 向SwitchDomain和healthyCheckResults结果
                        addResult(new HealthCheckResult(service.getName(), ip));

                        Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: {}",
                            cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                    } else {
                        if (!ip.isMockValid()) {
                            ip.setMockValid(true);
                            Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: {}",
                                cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                        }
                    }
                } else {
                    Loggers.EVT_LOG.info("serviceName: {} {OTHER} {IP-ENABLED} pre-valid: {}:{}@{} in {}, msg: {}",
                        cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), ip.getOKCount(), msg);
                }
            }
        } catch (Throwable t) {
            Loggers.SRV_LOG.error("[CHECK-OK] error when close check task.", t);
        }
        // 该实例的健康检查失败次数归零
        ip.getFailCount().set(0);
        // 将实例正在处于检查状态置为false
        ip.setBeingChecked(false);
    }

    /**
     * 在超过设定的失败次数之后，将实例设置为不健康状态
     * @param ip   健康检查失败的实例
     * @param task 健康检查任务
     * @param msg  健康检查信息
     */
    public void checkFail(Instance ip, HealthCheckTask task, String msg) {
        Cluster cluster = task.getCluster();

        try {
            // 如果实例目前正处于健康状态
            if (ip.isHealthy() || ip.isMockValid()) {
                // 和checkFailNow()不同点在于此，如果失败次数超过设定的失败次数，才会将实例置为不健康状态
                if (ip.getFailCount().incrementAndGet() >= switchDomain.getCheckTimes()) {
                    // 并且当前Nacos服务节点负责此实例
                    if (distroMapper.responsible(cluster, ip)) {
                        // 将实例置为不健康状态
                        ip.setHealthy(false);
                        ip.setMockValid(false);
                        // 更新service信息
                        Service service = cluster.getService();
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        addResult(new HealthCheckResult(service.getName(), ip));
                        // 发布service变更事件
                        pushService.serviceChanged(service);

                        Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-DISABLED} invalid: {}:{}@{}, region: {}, msg: {}",
                            cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                    } else {
                        Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-DISABLED} invalid: {}:{}@{}, region: {}, msg: {}",
                            cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                    }

                } else {
                    Loggers.EVT_LOG.info("serviceName: {} {OTHER} {IP-DISABLED} pre-invalid: {}:{}@{} in {}, msg: {}",
                        cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), ip.getFailCount(), msg);
                }
            }
        } catch (Throwable t) {
            Loggers.SRV_LOG.error("[CHECK-FAIL] error when close check task.", t);
        }
        // 该实例的健康检查成功次数归零
        ip.getOKCount().set(0);
        // 将实例正在处于检查状态置为false
        ip.setBeingChecked(false);
    }

    /**
     * 实例健康检查失败，立即将实例设置为不健康状态
     * @param ip   健康检查失败的实例
     * @param task 健康检查任务
     * @param msg  健康检查消息
     */
    public void checkFailNow(Instance ip, HealthCheckTask task, String msg) {
        Cluster cluster = task.getCluster();
        try {
            // 如果实例目前正处于健康状态
            if (ip.isHealthy() || ip.isMockValid()) {
                // 并且当前Nacos服务节点负责此实例
                if (distroMapper.responsible(cluster, ip)) {
                    // 将实例置为不健康状态
                    ip.setHealthy(false);
                    ip.setMockValid(false);
                    // 更新service信息
                    Service service = cluster.getService();
                    service.setLastModifiedMillis(System.currentTimeMillis());
                    // 发布service变更事件
                    pushService.serviceChanged(service);
                    addResult(new HealthCheckResult(service.getName(), ip));

                    Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-DISABLED} invalid-now: {}:{}@{}, region: {}, msg: {}",
                        cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                } else {
                    if (ip.isMockValid()) {
                        ip.setMockValid(false);
                        Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-DISABLED} invalid-now: {}:{}@{}, region: {}, msg: {}",
                            cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), UtilsAndCommons.LOCALHOST_SITE, msg);
                    }

                }
            }
        } catch (Throwable t) {
            Loggers.SRV_LOG.error("[CHECK-FAIL-NOW] error when close check task.", t);
        }
        // 该实例的健康检查成功次数归零
        ip.getOKCount().set(0);
        // 将实例正在处于检查状态置为false
        ip.setBeingChecked(false);
    }

    /**
     * 更新健康失败结果
     * @param result 健康检查结果
     */
    private void addResult(HealthCheckResult result) {
        if (!switchDomain.getIncrementalList().contains(result.getServiceName())) {
            return;
        }
        // 更新健康检查结果
        if (!healthCheckResults.offer(result)) {
            Loggers.EVT_LOG.warn("[HEALTH-CHECK-SYNC] failed to add check result to queue, queue size: {}", healthCheckResults.size());
        }
    }

    static class HealthCheckResult {
        private String serviceName;
        private Instance instance;

        public HealthCheckResult(String serviceName, Instance instance) {
            this.serviceName = serviceName;
            this.instance = instance;
        }

        public String getServiceName() {
            return serviceName;
        }

        public void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        public Instance getInstance() {
            return instance;
        }

        public void setInstance(Instance instance) {
            this.instance = instance;
        }
    }
}
