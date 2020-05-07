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

import com.alibaba.nacos.naming.misc.GlobalExecutor;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单个Raft节点
 * @author nacos
 */
public class RaftPeer {
    /**
     * Raft节点ip地址
     */
    public String ip;
    /**
     * Raft节点选举投票对象
     */
    public String voteFor;
    /**
     * 当前Raft节点所处于选举时段
     */
    public AtomicLong term = new AtomicLong(0L);

    /**
     * 随机生成数，区间[0, 15000)，单位ms
     *
     */
    public volatile long leaderDueMs = RandomUtils.nextLong(0, GlobalExecutor.LEADER_TIMEOUT_MS);

    public volatile long heartbeatDueMs = RandomUtils.nextLong(0, GlobalExecutor.HEARTBEAT_INTERVAL_MS);

    public State state = State.FOLLOWER;

    public void resetLeaderDue() {
        leaderDueMs = GlobalExecutor.LEADER_TIMEOUT_MS + RandomUtils.nextLong(0, GlobalExecutor.RANDOM_MS);
    }

    public void resetHeartbeatDue() {
        heartbeatDueMs = GlobalExecutor.HEARTBEAT_INTERVAL_MS;
    }

    public enum State {
        /**
         * 集群leader节点，一个集群仅有一个leader
         */
        LEADER,
        /**
         * 集群follower节点，向leader节点汇报，并从leader节点同步
         */
        FOLLOWER,
        /**
         * 候选leader节点，选举时才会出现的状态
         */
        CANDIDATE
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof RaftPeer)) {
            return false;
        }

        RaftPeer other = (RaftPeer) obj;

        return StringUtils.equals(ip, other.ip);
    }
}
