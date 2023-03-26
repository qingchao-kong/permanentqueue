/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.pockethub.permanentqueue;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerOffsetManager extends ConfigManager {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private ConcurrentMap<String/*topic*/, AtomicLong> committedOffsetTable = new ConcurrentHashMap<>();

    @JSONField(serialize = false)
    private ConcurrentMap<String/*topic*/, Long> nextReadOffsetTable = new ConcurrentHashMap<>();

    protected transient PermanentQueue permanentQueue;

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(PermanentQueue permanentQueue) {
        this.permanentQueue = permanentQueue;
    }

    public void commitOffset(final String topic, final long offset) {
        AtomicLong committedOffset = committedOffsetTable.computeIfAbsent(topic, k -> new AtomicLong(0));

        long prev;
        // the caller will not care about offsets going backwards, so we need to make sure we don't backtrack
        int i = 0;
        do {
            prev = committedOffset.get();
            // at least warn if this spins often, that would be a sign of very high contention, which should not happen
            if (++i % 10 == 0) {
                LOG.warn("Committing permanentQueue offset spins {} times now, this might be a bug. Continuing to try update.", i);
            }
        } while (!committedOffset.compareAndSet(prev, Math.max(offset, prev)));
    }

    /**
     * If the target queue has temporary reset offset, return the reset-offset.
     * Otherwise, return the current consume offset in the offset store.
     *
     * @param topic Topic
     * @return current consume offset or reset offset if there were one.
     */
    public long getNextReadOffset(final String topic) {
        return nextReadOffsetTable.computeIfAbsent(topic, k -> 0L);
    }

    public void setNextReadOffset(final String topic, long offset) {
        nextReadOffsetTable.put(topic, offset);
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return this.permanentQueue.getMessageStoreConfig().getStorePathRootDir() + File.separator + "config" + File.separator + "consumerOffset.json";
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.committedOffsetTable = obj.committedOffsetTable;
                for (Map.Entry<String, AtomicLong> entry : committedOffsetTable.entrySet()) {
                    this.nextReadOffsetTable.put(entry.getKey(), entry.getValue().get() + 1);
                }
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

}
