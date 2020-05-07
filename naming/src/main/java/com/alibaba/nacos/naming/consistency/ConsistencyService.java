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
package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.pojo.Record;

/**
 * 所有实现需要保证的一致性
 * <p>
 * 我们生命此一致性服务来使特定的一致性实现与业务逻辑脱钩
 * 开发者不应该关注当前使用的一致性协议
 * <p>
 * 以此方式，我们还为开发者提供了扩展基础一致性协议的空间，只要符合我们的一致性基础
 * @author nkorange
 * @since 1.0.0
 */
public interface ConsistencyService {

    /**
     * 写入Nacos集群的数据
     * @param key   数据key，全局唯一
     * @param value 数据
     * @throws NacosException
     * @see
     */
    void put(String key, Record value) throws NacosException;

    /**
     * 从Nacos集群数据
     * @param key 数据key
     * @throws NacosException
     */
    void remove(String key) throws NacosException;

    /**
     * 从Nacos集群获取数据
     * @param key 数据key
     * @return 查询的数据
     * @throws NacosException
     */
    Datum get(String key) throws NacosException;

    /**
     * 监听指定数据的变化
     * @param key      数据key
     * @param listener 数据发生变化时需要执行的回调任务
     * @throws NacosException
     */
    void listen(String key, RecordListener listener) throws NacosException;

    /**
     * 取消和指定数据关联的回调任务
     * @param key      数据key
     * @param listener 数据发生变化时需要执行的回调任务
     * @throws NacosException
     */
    void unlisten(String key, RecordListener listener) throws NacosException;

    /**
     * @return 获取当前的一致性状态
     */
    boolean isAvailable();
}
