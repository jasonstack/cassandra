/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * TODO could extend from {@code AbstractCell}
 */
public class ColumnInfo
{


    private final long timestamp;
    private final int ttl;
    private final int localDeletionTime;

    public static final ColumnInfo NO_TIMESTAMP = new ColumnInfo(LivenessInfo.NO_TIMESTAMP,
                                                                 Cell.NO_TTL,
                                                                 Cell.NO_DELETION_TIME);

    public ColumnInfo(long timestamp, int ttl, int localDeletionTime)
    {
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
    }

    public ColumnInfo merge(ColumnInfo other)
    {
        if (timestamp != other.timestamp)
            return timestamp > other.timestamp ? this : other;
        if (isTombstone() && !other.isTombstone())
            return this;
        if (!isTombstone() && other.isTombstone())
            return other;
        return localDeletionTime() > other.localDeletionTime() ? this : other;
    }

    public static Map<String, ColumnInfo> merge(Map<String, ColumnInfo> curr, Map<String, ColumnInfo> another)
    {
        // TODO refactor
        Map<String, ColumnInfo> merged = new HashMap<>();
        for (Map.Entry<String, ColumnInfo> entry : curr.entrySet())
            merged.merge(entry.getKey(), entry.getValue(), (c, o) -> c.merge(o));
        for (Map.Entry<String, ColumnInfo> entry : another.entrySet())
            merged.merge(entry.getKey(), entry.getValue(), (c, o) -> c.merge(o));
        return merged;
    }

    public static boolean anyTombstone(Map<String, ColumnInfo> info)
    {
        return info.values().stream().anyMatch(c -> c.isTombstone());
    }

    public static boolean anyTombstoneOrTTLed(Map<String, ColumnInfo> info, int nowInSec)
    {
        return info.values().stream().anyMatch(c -> !c.isLive(nowInSec));
    }

    public static boolean anyNotTombstone(Map<String, ColumnInfo> info)
    {
        return info.values().stream().anyMatch(c -> !c.isTombstone());
    }

    public static boolean anyAlive(Map<String, ColumnInfo> info, int nowInSec)
    {
        return info.values().stream().anyMatch(c -> c.isLive(nowInSec));
    }

    public long timestamp()
    {
        return timestamp;
    }

    public int ttl()
    {
        return ttl;
    }

    public int localDeletionTime()
    {
        return localDeletionTime;
    }

    public boolean isLive(int nowInSec)
    {
        return localDeletionTime() == Cell.NO_DELETION_TIME || (ttl() != Cell.NO_TTL && nowInSec < localDeletionTime());
    }

    public boolean isTombstone()
    {
        return localDeletionTime() != Cell.NO_DELETION_TIME && ttl() == Cell.NO_TTL;
    }

    public boolean isExpiring()
    {
        return ttl() != Cell.NO_TTL;

    }

    @Override
    public String toString()
    {
        return "ColumnInfo [timestamp=" + timestamp + ", ttl=" + ttl + ", localDeletionTime=" + localDeletionTime + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + localDeletionTime;
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        result = prime * result + ttl;
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ColumnInfo other = (ColumnInfo) obj;
        if (localDeletionTime != other.localDeletionTime)
            return false;
        if (timestamp != other.timestamp)
            return false;
        if (ttl != other.ttl)
            return false;
        return true;
    }
}
