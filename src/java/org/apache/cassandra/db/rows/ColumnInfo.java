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

import java.security.MessageDigest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.LivenessInfo;

/**
 * TODO could extend from {@code AbstractCell}
 */
public class ColumnInfo
{

    /**
     * Wrapper for ColumnInfos
     */
    public static class VirtualCells
    {
        public static final VirtualCells EMPTY = new VirtualCells();

        private final Map<String, ColumnInfo> keyOrConditions;
        private final Map<String, ColumnInfo> unselected;

        private VirtualCells()
        {
            keyOrConditions = Collections.EMPTY_MAP;
            unselected = Collections.EMPTY_MAP;
        }

        public VirtualCells(Map<String, ColumnInfo> keyOrConditions, Map<String, ColumnInfo> unselected)
        {
            this.keyOrConditions = keyOrConditions;
            this.unselected = unselected;
        }

        public static VirtualCells create(Map<String, ColumnInfo> keyOrConditions, Map<String, ColumnInfo> unselected)
        {
            if (keyOrConditions.isEmpty() && unselected.isEmpty())
                return EMPTY;
            return new VirtualCells(keyOrConditions, unselected);
        }

        public boolean isEmpty()
        {
            return keyOrConditions.isEmpty() && unselected.isEmpty();
        }

        public Map<String, ColumnInfo> getKeyOrConditions()
        {
            return keyOrConditions;
        }

        public Map<String, ColumnInfo> getUnselected()
        {
            return unselected;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((keyOrConditions == null) ? 0 : keyOrConditions.hashCode());
            result = prime * result + ((unselected == null) ? 0 : unselected.hashCode());
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
            VirtualCells other = (VirtualCells) obj;
            if (keyOrConditions == null)
            {
                if (other.keyOrConditions != null)
                    return false;
            }
            else if (!keyOrConditions.equals(other.keyOrConditions))
                return false;
            if (unselected == null)
            {
                if (other.unselected != null)
                    return false;
            }
            else if (!unselected.equals(other.unselected))
                return false;
            return true;
        }

        public VirtualCells merge(VirtualCells another)
        {
            if (isEmpty())
                return another;
            if (another.isEmpty())
                return this;

            assert getKeyOrConditions().size() == another.getKeyOrConditions().size();
            // TODO optimize, if empty, no need to merge
            Map<String, ColumnInfo> mergedKeyOrConditions = new HashMap<>();
            for (Map.Entry<String, ColumnInfo> entry : getKeyOrConditions().entrySet())
            {
                String column = entry.getKey();
                ColumnInfo left = entry.getValue();
                ColumnInfo right = another.getKeyOrConditions().get(column);
                assert right != null; // both map should contains same key
                ColumnInfo mergedColumnInfo = left.merge(right);
                mergedKeyOrConditions.put(column, mergedColumnInfo);
            }

            Map<String, ColumnInfo> mergedUnselected = new HashMap<>();
            getUnselected().entrySet()
                           .forEach(e -> mergedUnselected.merge(e.getKey(), e.getValue(), (c1, c2) -> c1.merge(c2)));
            another.getUnselected()
                   .entrySet()
                   .forEach(e -> mergedUnselected.merge(e.getKey(), e.getValue(), (c1, c2) -> c1.merge(c2)));

            return VirtualCells.create(mergedKeyOrConditions, mergedUnselected);
        }

        public boolean shouldWipeRow(int nowInSeconds)
        {
            return getKeyOrConditions().values().stream().anyMatch(c -> !c.isLive(nowInSeconds));
        }

        @Override
        public String toString()
        {
            return "VirtualCells [keyOrConditions=" + keyOrConditions + ", unselected=" + unselected + "]";
        }

        public int dataSize()
        {
            // FIXME
            return 0;
        }

        public void digest(MessageDigest digest)
        {
            // FIXME
        }

    }


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
