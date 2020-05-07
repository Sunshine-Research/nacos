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

import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * TCP健康检查处理器
 * @author nacos
 */
@Component
public class TcpSuperSenseProcessor implements HealthCheckProcessor, Runnable {

    public static final String TYPE = "TCP";

    @Autowired
    private HealthCheckCommon healthCheckCommon;

    @Autowired
    private SwitchDomain switchDomain;

    public static final int CONNECT_TIMEOUT_MS = 500;

    private Map<String, BeatKey> keyMap = new ConcurrentHashMap<>();
    /**
     * 任务队列
     */
    private BlockingQueue<Beat> taskQueue = new LinkedBlockingQueue<Beat>();

    /**
     * 此值已经经过仔细的调整，如果你有足够的自信，可以进行更改
     */
    private static final int NIO_THREAD_COUNT = Runtime.getRuntime().availableProcessors() <= 1 ?
        1 : Runtime.getRuntime().availableProcessors() / 2;

    /**
     * 因为一些节点不支持keep-alive连接，临时关闭
     */
    private static final long TCP_KEEP_ALIVE_MILLIS = 0;

    private static ScheduledExecutorService TCP_CHECK_EXECUTOR
        = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("nacos.naming.tcp.check.worker");
            t.setDaemon(true);
            return t;
        }
    });

    private static ScheduledExecutorService NIO_EXECUTOR
        = Executors.newScheduledThreadPool(NIO_THREAD_COUNT,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("nacos.supersense.checker");
                return thread;
            }
        }
    );

    private Selector selector;

    public TcpSuperSenseProcessor() {
        try {
            selector = Selector.open();

            TCP_CHECK_EXECUTOR.submit(this);

        } catch (Exception e) {
            throw new IllegalStateException("Error while initializing SuperSense(TM).");
        }
    }

    @Override
    public void process(HealthCheckTask task) {
        // 获取当前cluster下所有持久实例
        List<Instance> ips = task.getCluster().allIPs(false);

        if (CollectionUtils.isEmpty(ips)) {
            return;
        }
        // 遍历所有持久实例
        for (Instance ip : ips) {

            if (ip.isMarked()) {
                if (SRV_LOG.isDebugEnabled()) {
                    SRV_LOG.debug("tcp check, ip is marked as to skip health check, ip:" + ip.getIp());
                }
                continue;
            }
            // 已经标记为正在检查，则不进行处理
            if (!ip.markChecking()) {
                SRV_LOG.warn("tcp check started before last one finished, service: "
                    + task.getCluster().getService().getName() + ":"
                    + task.getCluster().getName() + ":"
                    + ip.getIp() + ":"
                    + ip.getPort());

                healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task, switchDomain.getTcpHealthParams());
                continue;
            }
            // 构建心跳任务，并添加到心跳队列中
            Beat beat = new Beat(ip, task);
            taskQueue.add(beat);
            // 递增TCP健康检查计数器
            MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
        }
    }

    private void processTask() throws Exception {
        Collection<Callable<Void>> tasks = new LinkedList<>();
        do {
            // 在任务队列中存在任务，并且任务数量小于指定计算个数的前提下进行自旋
            Beat beat = taskQueue.poll(CONNECT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
            if (beat == null) {
                return;
            }
            // 向本地缓存中添加取出的任务
            tasks.add(new TaskProcessor(beat));
        } while (taskQueue.size() > 0 && tasks.size() < NIO_THREAD_COUNT * 64);
        // 使用NIO线程池处理取出的任务
        for (Future<?> f : NIO_EXECUTOR.invokeAll(tasks)) {
            // 这么做可以抛出异常
            f.get();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                // 处理任务队列中的任务
                processTask();

                int readyCount = selector.selectNow();
                if (readyCount <= 0) {
                    continue;
                }
                // 遍历NIO模型下的心跳请求
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    // 添加到心跳请求到线程池中执行
                    NIO_EXECUTOR.execute(new PostProcessor(key));
                }
            } catch (Throwable e) {
                SRV_LOG.error("[HEALTH-CHECK] error while processing NIO task", e);
            }
        }
    }

    /**
     * 实例发送的心跳请求
     */
    public class PostProcessor implements Runnable {
        SelectionKey key;

        public PostProcessor(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            // 获取心跳信息
            Beat beat = (Beat) key.attachment();
            // 获取写入的channel
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                if (!beat.isHealthy()) {
                    // 非法的心跳意味着server节点不再负责此service
                    key.cancel();
                    key.channel().close();
                    // 仅结束健康检查状态，不更新实例状态，等待同步其他Nacos节点数据
                    beat.finishCheck();
                    return;
                }
                // 结束channel，并设置
                if (key.isValid() && key.isConnectable()) {
                    // 结束心跳请求
                    channel.finishConnect();
                    beat.finishCheck(true, false, System.currentTimeMillis() - beat.getTask().getStartTime(), "tcp:ok+");
                }

                if (key.isValid() && key.isReadable()) {
                    // 断开连接
                    ByteBuffer buffer = ByteBuffer.allocate(128);
                    if (channel.read(buffer) == -1) {
                        key.cancel();
                        key.channel().close();
                    } else {
                        // 不中断请求，进行忽略
                    }
                }
            } catch (ConnectException e) {
                // 无法连接，可能端口还未开启
                beat.finishCheck(false, true, switchDomain.getTcpHealthParams().getMax(), "tcp:unable2connect:" + e.getMessage());
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());
                // 其他情况，直接关闭channel
                try {
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    private class Beat {
        // 心跳实例
        Instance ip;
        // 心跳检查任务
        HealthCheckTask task;

        long startTime = System.currentTimeMillis();

        Beat(Instance ip, HealthCheckTask task) {
            this.ip = ip;
            this.task = task;
        }

        public void setStartTime(long time) {
            startTime = time;
        }

        public long getStartTime() {
            return startTime;
        }

        public Instance getIp() {
            return ip;
        }

        public HealthCheckTask getTask() {
            return task;
        }

        public boolean isHealthy() {
            return System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(30L);
        }

        /**
         * 仅结束实例健康状态检查，不设置实例的状态
         */
        public void finishCheck() {
            ip.setBeingChecked(false);
        }

        /**
         * 结束实例健康状态检查
         * @param success
         * @param now
         * @param rt
         * @param msg
         */
        public void finishCheck(boolean success, boolean now, long rt, String msg) {
            // 设置当前实例本次健康检查频率
            ip.setCheckRT(System.currentTimeMillis() - startTime);

            if (success) {
                // 健康检查通过
                healthCheckCommon.checkOK(ip, task, msg);
            } else {
                if (now) {
                    // 立即将实例设置为健康检查失败
                    healthCheckCommon.checkFailNow(ip, task, msg);
                } else {
                    // 健康检查失败
                    healthCheckCommon.checkFail(ip, task, msg);
                }
                // 从当前任务缓存中移除当前任务
                keyMap.remove(task.toString());
            }
            // 重新计算健康检查频率
            healthCheckCommon.reEvaluateCheckRT(rt, task, switchDomain.getTcpHealthParams());
        }

        @Override
        public String toString() {
            return task.getCluster().getService().getName() + ":"
                + task.getCluster().getName() + ":"
                + ip.getIp() + ":"
                + ip.getPort();
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip.toJSON());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Beat)) {
                return false;
            }

            return this.toString().equals(obj.toString());
        }
    }

    private static class BeatKey {
        public SelectionKey key;
        public long birthTime;

        public BeatKey(SelectionKey key) {
            this.key = key;
            this.birthTime = System.currentTimeMillis();
        }
    }

    private static class TimeOutTask implements Runnable {
        SelectionKey key;

        public TimeOutTask(SelectionKey key) {
            this.key = key;
        }

        @Override
        public void run() {
            if (key != null && key.isValid()) {
                SocketChannel channel = (SocketChannel) key.channel();
                Beat beat = (Beat) key.attachment();

                if (channel.isConnected()) {
                    return;
                }

                try {
                    channel.finishConnect();
                } catch (Exception ignore) {
                }

                try {
                    beat.finishCheck(false, false, beat.getTask().getCheckRTNormalized() * 2, "tcp:timeout");
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    private class TaskProcessor implements Callable<Void> {

        private static final int MAX_WAIT_TIME_MILLISECONDS = 500;
        Beat beat;

        public TaskProcessor(Beat beat) {
            this.beat = beat;
        }

        @Override
        public Void call() {
            long waited = System.currentTimeMillis() - beat.getStartTime();
            if (waited > MAX_WAIT_TIME_MILLISECONDS) {
                Loggers.SRV_LOG.warn("beat task waited too long: " + waited + "ms");
            }

            SocketChannel channel = null;
            try {
                Instance instance = beat.getIp();
                Cluster cluster = beat.getTask().getCluster();

                BeatKey beatKey = keyMap.get(beat.toString());
                if (beatKey != null && beatKey.key.isValid()) {
                    if (System.currentTimeMillis() - beatKey.birthTime < TCP_KEEP_ALIVE_MILLIS) {
                        instance.setBeingChecked(false);
                        return null;
                    }

                    beatKey.key.cancel();
                    beatKey.key.channel().close();
                }

                channel = SocketChannel.open();
                channel.configureBlocking(false);
                // only by setting this can we make the socket close event asynchronous
                channel.socket().setSoLinger(false, -1);
                channel.socket().setReuseAddress(true);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);

                int port = cluster.isUseIPPort4Check() ? instance.getPort() : cluster.getDefCkport();
                channel.connect(new InetSocketAddress(instance.getIp(), port));

                SelectionKey key
                    = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                key.attach(beat);
                keyMap.put(beat.toString(), new BeatKey(key));

                beat.setStartTime(System.currentTimeMillis());

                NIO_EXECUTOR.schedule(new TimeOutTask(key),
                    CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());

                if (channel != null) {
                    try {
                        channel.close();
                    } catch (Exception ignore) {
                    }
                }
            }

            return null;
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
