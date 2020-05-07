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
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * @author nacos
 */
@Component
public class RaftStore {

    /**
     * meta.properties属性文件
     */
    private Properties meta = new Properties();

    private String metaFileName = UtilsAndCommons.DATA_BASE_DIR + File.separator + "meta.properties";

    private String cacheDir = UtilsAndCommons.DATA_BASE_DIR + File.separator + "data";

    public synchronized void loadDatums(RaftCore.Notifier notifier, ConcurrentMap<String, Datum> datums) throws Exception {

        Datum datum;
        long start = System.currentTimeMillis();
        for (File cache : listCaches()) {
            if (cache.isDirectory() && cache.listFiles() != null) {
                // 加载所有的缓存数据文件
                for (File datumFile : cache.listFiles()) {
                    datum = readDatum(datumFile, cache.getName());
                    if (datum != null) {
                        // 放入到数据缓存中，并添加通知器任务
                        datums.put(datum.key, datum);
                        notifier.addTask(datum.key, ApplyAction.CHANGE);
                    }
                }
                continue;
            }
            // 如果cache是个文件，读取cache文件，并添加数据缓存
            datum = readDatum(cache, StringUtils.EMPTY);
            if (datum != null) {
                datums.put(datum.key, datum);
            }
        }

        Loggers.RAFT.info("finish loading all datums, size: {} cost {} ms.", datums.size(), (System.currentTimeMillis() - start));
    }

    /**
     * 加载本地缓存数据
     * @return 本地缓存属性
     * @throws Exception
     */
    public synchronized Properties loadMeta() throws Exception {
        // 读取NacosHome下meta.properties文件
        File metaFile = new File(metaFileName);
        if (!metaFile.exists() && !metaFile.getParentFile().mkdirs() && !metaFile.createNewFile()) {
            throw new IllegalStateException("failed to create meta file: " + metaFile.getAbsolutePath());
        }

        try (FileInputStream inStream = new FileInputStream(metaFile)) {
            // 加载属性文件
            meta.load(inStream);
        }
        return meta;
    }

    public synchronized Datum load(String key) throws Exception {
        long start = System.currentTimeMillis();
        // load data
        for (File cache : listCaches()) {
            if (!cache.isFile()) {
                Loggers.RAFT.warn("warning: encountered directory in cache dir: {}", cache.getAbsolutePath());
            }

            if (!StringUtils.equals(cache.getName(), encodeFileName(key))) {
                continue;
            }

            Loggers.RAFT.info("finish loading datum, key: {} cost {} ms.",
                key, (System.currentTimeMillis() - start));
            return readDatum(cache, StringUtils.EMPTY);
        }

        return null;
    }

    /**
     * 读取数据
     * @param file 数据来源文件
     * @param namespaceId 来源namespace
     * @return 读取出的数据
     * @throws IOException
     */
    public synchronized Datum readDatum(File file, String namespaceId) throws IOException {

        ByteBuffer buffer;
        FileChannel fc = null;
        try {
            fc = new FileInputStream(file).getChannel();
            buffer = ByteBuffer.allocate((int) file.length());
            fc.read(buffer);

            String json = new String(buffer.array(), StandardCharsets.UTF_8);
            if (StringUtils.isBlank(json)) {
                return null;
            }

            if (KeyBuilder.matchSwitchKey(file.getName())) {
                return JSON.parseObject(json, new TypeReference<Datum<SwitchDomain>>() {
                });
            }

            // 如果是以"com.alibaba.nacos.naming.domains.meta."和"meta."开头的文件名称，则视为service名称
            if (KeyBuilder.matchServiceMetaKey(file.getName())) {
                // 当前的数据则为service数据
                Datum<Service> serviceDatum;

                try {
                    serviceDatum = JSON.parseObject(json.replace("\\", ""), new TypeReference<Datum<Service>>() {
                    });
                } catch (Exception e) {
                    JSONObject jsonObject = JSON.parseObject(json);

                    serviceDatum = new Datum<>();
                    serviceDatum.timestamp.set(jsonObject.getLongValue("timestamp"));
                    serviceDatum.key = jsonObject.getString("key");
                    serviceDatum.value = JSON.parseObject(jsonObject.getString("value"), Service.class);
                }

                // 设置service的group集群名称
                if (StringUtils.isBlank(serviceDatum.value.getGroupName())) {
                    serviceDatum.value.setGroupName(Constants.DEFAULT_GROUP);
                }
                // 设置默认service名称，"DEFAULT_GROUP" + @@ + service名称
                if (!serviceDatum.value.getName().contains(Constants.SERVICE_INFO_SPLITER)) {
                    serviceDatum.value.setName(Constants.DEFAULT_GROUP
                        + Constants.SERVICE_INFO_SPLITER + serviceDatum.value.getName());
                }

                return serviceDatum;
            }

            // 如果是instance，文件以"com.alibaba.nacos.naming.iplist."和"iplist."开头
            if (KeyBuilder.matchInstanceListKey(file.getName())) {
                // 则解析为实例数据
                Datum<Instances> instancesDatum;

                try {
                    instancesDatum = JSON.parseObject(json, new TypeReference<Datum<Instances>>() {
                    });
                } catch (Exception e) {
                    JSONObject jsonObject = JSON.parseObject(json);
                    instancesDatum = new Datum<>();
                    instancesDatum.timestamp.set(jsonObject.getLongValue("timestamp"));

                    String key = jsonObject.getString("key");
                    String serviceName = KeyBuilder.getServiceName(key);
                    key = key.substring(0, key.indexOf(serviceName)) +
                        Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER + serviceName;

                    instancesDatum.key = key;
                    instancesDatum.value = new Instances();
                    instancesDatum.value.setInstanceList(JSON.parseObject(jsonObject.getString("value"),
                        new TypeReference<List<Instance>>() {
                        }));
                    if (!instancesDatum.value.getInstanceList().isEmpty()) {
                        for (Instance instance : instancesDatum.value.getInstanceList()) {
                            instance.setEphemeral(false);
                        }
                    }
                }

                return instancesDatum;
            }

            return JSON.parseObject(json, Datum.class);

        } catch (Exception e) {
            Loggers.RAFT.warn("waning: failed to deserialize key: {}", file.getName());
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }
    }

    /**
     * 持久化数据
     * @param datum 需要进行持久化的数据
     * @throws Exception
     */
    public synchronized void write(final Datum datum) throws Exception {

        // 获取数据的namespace
        String namespaceId = KeyBuilder.getNamespace(datum.key);

        File cacheFile;

        if (StringUtils.isNotBlank(namespaceId)) {
            // 使用namespace+数据名称构建文件地址
            cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
        } else {
            // 直接使用数据名称构建文件地址
            cacheFile = new File(cacheDir + File.separator + encodeFileName(datum.key));
        }
        // 如果无法写入磁盘，记录为磁盘异常
        if (!cacheFile.exists() && !cacheFile.getParentFile().mkdirs() && !cacheFile.createNewFile()) {
            MetricsMonitor.getDiskException().increment();

            throw new IllegalStateException("can not make cache file: " + cacheFile.getName());
        }
        // 使用NIO模式写入文件
        FileChannel fc = null;
        ByteBuffer data;
        // 构建堆外内存缓冲区
        data = ByteBuffer.wrap(JSON.toJSONString(datum).getBytes(StandardCharsets.UTF_8));

        try {
            fc = new FileOutputStream(cacheFile, false).getChannel();
            // 写入到缓冲区
            fc.write(data, data.position());
            // 强制刷新
            fc.force(true);
        } catch (Exception e) {
            MetricsMonitor.getDiskException().increment();
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }

        // 移除旧格式的文件
        if (StringUtils.isNoneBlank(namespaceId)) {
            // 主要针对service的旧数据
            if (datum.key.contains(Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER)) {
                // 获取旧格式数据的key
                String oldFormatKey =
                    datum.key.replace(Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER, StringUtils.EMPTY);
                // 创建旧格式数据的文件
                cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(oldFormatKey));
                // 对旧格式数据进行删除
                if (cacheFile.exists() && !cacheFile.delete()) {
                    Loggers.RAFT.error("[RAFT-DELETE] failed to delete old format datum: {}, value: {}",
                        datum.key, datum.value);
                    throw new IllegalStateException("failed to delete old format datum: " + datum.key);
                }
            }
        }
    }

    private File[] listCaches() throws Exception {
        // 获取所有的缓存的数据
        File cacheDir = new File(this.cacheDir);
        if (!cacheDir.exists() && !cacheDir.mkdirs()) {
            throw new IllegalStateException("cloud not make out directory: " + cacheDir.getName());
        }

        return cacheDir.listFiles();
    }

    public void delete(Datum datum) {
        // 获取数据的namespace
        String namespaceId = KeyBuilder.getNamespace(datum.key);

        if (StringUtils.isNotBlank(namespaceId)) {
            // 获取文件地址，并删除
            File cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
            if (cacheFile.exists() && !cacheFile.delete()) {
                Loggers.RAFT.error("[RAFT-DELETE] failed to delete datum: {}, value: {}", datum.key, datum.value);
                throw new IllegalStateException("failed to delete datum: " + datum.key);
            }
        }
    }

    /**
     * 更新元数据
     * @param term
     * @throws Exception
     */
    public void updateTerm(long term) throws Exception {
        File file = new File(metaFileName);
        if (!file.exists() && !file.getParentFile().mkdirs() && !file.createNewFile()) {
            throw new IllegalStateException("failed to create meta file");
        }

        try (FileOutputStream outStream = new FileOutputStream(file)) {
            // 写入元数据
            meta.setProperty("term", String.valueOf(term));
            meta.store(outStream, null);
        }
    }

    private static String encodeFileName(String fileName) {
        return fileName.replace(':', '#');
    }

    private static String decodeFileName(String fileName) {
        return fileName.replace("#", ":");
    }
}
