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
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.FBUtilities;

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

    public static boolean anyTombstone(Map<ByteBuffer, ColumnInfo> info)
    {
        return info.values().stream().anyMatch(c -> c.isTombstone());
    }

    public static boolean anyTombstoneOrTTLed(Map<ByteBuffer, ColumnInfo> info, int nowInSec)
    {
        return info.values().stream().anyMatch(c -> !c.isLive(nowInSec));
    }

    public static boolean anyNotTombstone(Map<ByteBuffer, ColumnInfo> info)
    {
        return info.values().stream().anyMatch(c -> !c.isTombstone());
    }

    public static boolean anyAlive(Map<ByteBuffer, ColumnInfo> info, int nowInSec)
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
    public boolean equals(Object other)
    {
        if (!(other instanceof ColumnInfo))
            return false;

        ColumnInfo that = (ColumnInfo) other;
        return this.timestamp() == that.timestamp()
                && this.ttl() == that.ttl()
                && this.localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timestamp(), ttl(), localDeletionTime());
    }

    public int dataSize()
    {
        return TypeSizes.sizeof(timestamp()) + TypeSizes.sizeof(ttl()) + TypeSizes.sizeof(localDeletionTime());
    }

    public void digest(MessageDigest digest)
    {
        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithInt(digest, ttl());
    }
}
