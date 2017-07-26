package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Wrapper for ColumnInfos. TODO optimize in-memory and storage representation
 */
public class VirtualCells
{
    public static final VirtualCells EMPTY = new VirtualCells(Collections.emptyMap(), Collections.emptyMap());

    public static final Serializer serializer = new VirtualCells.Serializer();

    private final Map<String, ColumnInfo> keyOrConditions;
    private final Map<String, ColumnInfo> unselected;

    private VirtualCells(Map<String, ColumnInfo> keyOrConditions, Map<String, ColumnInfo> unselected)
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

    public Map<String, ColumnInfo> keyOrConditions()
    {
        return keyOrConditions;
    }

    public Map<String, ColumnInfo> unselected()
    {
        return unselected;
    }

    public boolean anyLiveUnselected(int nowInSec)
    {
        return ColumnInfo.anyAlive(unselected(), nowInSec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyOrConditions(), unselected());
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

    /**
     * Merge internal column info based on timestamp/tombstone.
     * 
     * @param another
     * @return
     */
    public VirtualCells merge(VirtualCells another)
    {
        if (isEmpty())
            return another;
        if (another.isEmpty())
            return this;

        Map<String, ColumnInfo> mergedKeyOrConditions = merge(keyOrConditions(), another.keyOrConditions());

        Map<String, ColumnInfo> mergedUnselected = merge(unselected(), another.unselected());

        return VirtualCells.create(mergedKeyOrConditions, mergedUnselected);
    }

    private static Map<String, ColumnInfo> merge(Map<String, ColumnInfo> left,
                                                                Map<String, ColumnInfo> right)
    {
        if (left.isEmpty())
            return right;
        if (right.isEmpty())
            return left;
        Map<String, ColumnInfo> mergedKeyOrConditions = new HashMap<>();
        left.entrySet()
            .forEach(e -> mergedKeyOrConditions.merge(e.getKey(), e.getValue(), (c1, c2) -> c1.merge(c2)));
        right.entrySet()
             .forEach(e -> mergedKeyOrConditions.merge(e.getKey(), e.getValue(), (c1, c2) -> c1.merge(c2)));
        return mergedKeyOrConditions;
    }

    /**
     * If any base normal column used in primary key or used in view's filter condition is not alive, entire view row is
     * considered dead.
     * 
     * @param nowInSeconds
     * @return
     */
    public boolean shouldWipeRow(int nowInSeconds)
    {
        return keyOrConditions().values().stream().anyMatch(c -> !c.isLive(nowInSeconds));
    }

    @Override
    public String toString()
    {
        return "VirtualCells [keyOrConditions=" + keyOrConditions + ", unselected=" + unselected + "]";
    }

    public int dataSize()
    {
        int size = 0;
        size += dataSize(keyOrConditions());
        size += dataSize(unselected());
        return size;
    }

    private static int dataSize(Map<String, ColumnInfo> payload)
    {
        int size = 0;
        for (Map.Entry<String, ColumnInfo> entry : payload.entrySet())
        {
            size += TypeSizes.sizeof(entry.getKey());
            size += entry.getValue().dataSize();
        }
        return size;
    }

    public void digest(MessageDigest digest)
    {
        digest(keyOrConditions(), digest);
        digest(unselected(), digest);
    }

    private void digest(Map<String, ColumnInfo> payload, MessageDigest digest)
    {
        if (payload.isEmpty())
            return;
        Object[] keys = payload.keySet().toArray();
        Arrays.sort(keys);
        for (Object key : keys)
        {
            // FIXME digest column-identifier
            payload.get(key).digest(digest);
        }
    }

    static class Serializer
    {
        public void serialize(VirtualCells virtualCells,
                              DataOutputPlus out,
                              SerializationHeader header) throws IOException
        {
            assert !virtualCells.isEmpty();
            serializeRowVirtualCellsPayload(virtualCells.keyOrConditions(), header, out);
            serializeRowVirtualCellsPayload(virtualCells.unselected(), header, out);
        }

        private void serializeRowVirtualCellsPayload(Map<String, ColumnInfo> payload,
                                                     SerializationHeader header,
                                                     DataOutputPlus out) throws IOException
        {
            out.writeInt(payload.size());
            for (Map.Entry<String, ColumnInfo> data : payload.entrySet())
            {
                ColumnInfo info = data.getValue();
                out.writeUTF(data.getKey());
                header.writeTTL(info.ttl(), out);
                header.writeTimestamp(info.timestamp(), out);
                header.writeLocalDeletionTime(info.localDeletionTime(), out);
            }
        }

        public VirtualCells deserialize(DataInputPlus in, SerializationHeader header) throws IOException
        {
            Map<String, ColumnInfo> keyOrConditions = deserializeRowVirtualCellsPayload(header, in);
            Map<String, ColumnInfo> unselected = deserializeRowVirtualCellsPayload(header, in);

            return VirtualCells.create(keyOrConditions, unselected);
        }

        private Map<String, ColumnInfo> deserializeRowVirtualCellsPayload(SerializationHeader header,
                                                                          DataInputPlus in) throws IOException
        {
            Map<String, ColumnInfo> payload = new HashMap<>();
            int size = in.readInt();
            for (int i = 0; i < size; i++)
            {
                String key = in.readUTF();
                int ttl = header.readTTL(in);
                long timestamp = header.readTimestamp(in);
                int localDeletionTime = header.readLocalDeletionTime(in);
                payload.put(key, new ColumnInfo(timestamp, ttl, localDeletionTime));
            }
            return payload;
        }

        public long serializedSize(VirtualCells virtualCells, SerializationHeader header)
        {
            long size = 0;
            if (!virtualCells.isEmpty())
            {
                Map<String, ColumnInfo> keyOrConditions = virtualCells.keyOrConditions();
                size += serializeRowVirtualCellsPayloadSize(keyOrConditions, header);

                Map<String, ColumnInfo> unselected = virtualCells.unselected();
                size += serializeRowVirtualCellsPayloadSize(unselected, header);
            }
            return size;
        }

        private long serializeRowVirtualCellsPayloadSize(Map<String, ColumnInfo> payload, SerializationHeader header)
        {
            long size = 0;
            size += TypeSizes.sizeof(payload.size());
            for (Map.Entry<String, ColumnInfo> data : payload.entrySet())
            {
                ColumnInfo info = data.getValue();
                size += header.timestampSerializedSize(info.timestamp());

                size += header.localDeletionTimeSerializedSize(info.localDeletionTime());
                size += header.ttlSerializedSize(info.ttl());
            }
            return size;
        }
    }

    public ColumnInfo maxUnselectedColumn()
    {
        ColumnInfo max = null;
        for (Map.Entry<String, ColumnInfo> data : unselected().entrySet())
        {
            ColumnInfo info = data.getValue();
            max = max == null ? info : max.merge(info);
        }
        return max;
    }

    public ColumnInfo maxKeyOrConditionsDeadColumn(int nowInSec)
    {
        ColumnInfo max = null;
        for (Map.Entry<String, ColumnInfo> data : keyOrConditions().entrySet())
        {
            ColumnInfo info = data.getValue();
            if (!info.isLive(nowInSec))
                max = max == null ? info : max.merge(info); // greatest dead column
        }
        return max;
    }
}