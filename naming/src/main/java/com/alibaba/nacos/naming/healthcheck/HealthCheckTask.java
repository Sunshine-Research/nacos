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
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.RandomUtils;

/**
 * 健康检查任务
 * @author nacos
 */
public class HealthCheckTask implements Runnable {
    /**
     * 健康检查任务关联的集群
     */
    private Cluster cluster;

    private long checkRTNormalized = -1;
    private long checkRTBest = -1;
    private long checkRTWorst = -1;

    private long checkRTLast = -1;
    private long checkRTLastLast = -1;

    private long startTime;

    private volatile boolean cancelled = false;

    @JSONField(serialize = false)
    private DistroMapper distroMapper;

    @JSONField(serialize = false)
    private SwitchDomain switchDomain;

    @JSONField(serialize = false)
    private HealthCheckProcessor healthCheckProcessor;

    public HealthCheckTask(Cluster cluster) {
        this.cluster = cluster;
        // 初始化server变更listener
        distroMapper = SpringContext.getAppContext().getBean(DistroMapper.class);
        // 获取全局的SwitchDomain
        switchDomain = SpringContext.getAppContext().getBean(SwitchDomain.class);
        // 健康检查代理类，用于发起健康检查
        healthCheckProcessor = SpringContext.getAppContext().getBean(HealthCheckProcessorDelegate.class);
        // 初始化健康检查的时间间隔
        initCheckRT();
    }

    /**
     * 初始化检查频率
     */
    public void initCheckRT() {
        // 检查的延迟时间，2s+(0, 5)之间的随机数
        checkRTNormalized = 2000 + RandomUtils.nextInt(0, RandomUtils.nextInt(0, switchDomain.getTcpHealthParams().getMax()));
        checkRTBest = Long.MAX_VALUE;
        checkRTWorst = 0L;
    }

    @Override
    public void run() {

        try {
            // 如果当前的distro负责本数据块的任务，并且开启了当前service支持健康检查
            if (distroMapper.responsible(cluster.getService().getName()) &&
                switchDomain.isHealthCheckEnabled(cluster.getService().getName())) {
                // 调度执行健康检查任务
                healthCheckProcessor.process(this);
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[HEALTH-CHECK] schedule health check task: {}", cluster.getService().getName());
                }
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[HEALTH-CHECK] error while process health check for {}:{}",
                cluster.getService().getName(), cluster.getName(), e);
        } finally {
            // 如果任务没有取消
            if (!cancelled) {
                // 调度下一次健康检查任务
                HealthCheckReactor.scheduleCheck(this);

                // worst == 0意味着没有进行过健康检查
                if (this.getCheckRTWorst() > 0
                    && switchDomain.isHealthCheckEnabled(cluster.getService().getName())
                    && distroMapper.responsible(cluster.getService().getName())) {
                    // 计算两次健康检查时间间隔
                    long diff = ((this.getCheckRTLast() - this.getCheckRTLastLast()) * 10000)
                        / this.getCheckRTLastLast();

                    this.setCheckRTLastLast(this.getCheckRTLast());

                    Cluster cluster = this.getCluster();

                    if (Loggers.CHECK_RT.isDebugEnabled()) {
                        Loggers.CHECK_RT.debug("{}:{}@{}->normalized: {}, worst: {}, best: {}, last: {}, diff: {}",
                            cluster.getService().getName(), cluster.getName(), cluster.getHealthChecker().getType(),
                            this.getCheckRTNormalized(), this.getCheckRTWorst(), this.getCheckRTBest(),
                            this.getCheckRTLast(), diff);
                    }
                }
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public long getCheckRTNormalized() {
        return checkRTNormalized;
    }

    public long getCheckRTBest() {
        return checkRTBest;
    }

    public long getCheckRTWorst() {
        return checkRTWorst;
    }

    public void setCheckRTWorst(long checkRTWorst) {
        this.checkRTWorst = checkRTWorst;
    }

    public void setCheckRTBest(long checkRTBest) {
        this.checkRTBest = checkRTBest;
    }

    public void setCheckRTNormalized(long checkRTNormalized) {
        this.checkRTNormalized = checkRTNormalized;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getCheckRTLast() {
        return checkRTLast;
    }

    public void setCheckRTLast(long checkRTLast) {
        this.checkRTLast = checkRTLast;
    }

    public long getCheckRTLastLast() {
        return checkRTLastLast;
    }

    public void setCheckRTLastLast(long checkRTLastLast) {
        this.checkRTLastLast = checkRTLastLast;
    }
}
