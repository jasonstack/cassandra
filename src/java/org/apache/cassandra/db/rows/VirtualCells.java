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
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DroppedColumn;

/**
 * Wrapper for ColumnInfos. TODO optimize in-memory and storage representation
 */
public class VirtualCells
{
    public static final VirtualCells EMPTY = new VirtualCells(Collections.emptyMap(), false);

    public static final Serializer serializer = new VirtualCells.Serializer();

    // use bytebuffer instead
    private final Map<ByteBuffer, ColumnInfo> payload;
    private final boolean isKeyOrFilter; // if true and payload has dead column, wipe entire row; if false and payload
                                         // has any column alive, view row alive.

    private VirtualCells(Map<ByteBuffer, ColumnInfo> payload, boolean isKeyOrFilter)
    {
        this.payload = payload;
        this.isKeyOrFilter = isKeyOrFilter;
    }

    private static VirtualCells create(Map<ByteBuffer, ColumnInfo> payload, boolean isKeyOrFilter)
    {
        if (payload.isEmpty())
            return EMPTY;
        return new VirtualCells(payload, isKeyOrFilter);
    }

    public static VirtualCells createKeyOrCondition(Map<ByteBuffer, ColumnInfo> keyOrConditions)
    { 
        if (keyOrConditions.isEmpty())
            return EMPTY;
        return new VirtualCells(keyOrConditions, true);
    }

    public static VirtualCells createUnselected(Map<ByteBuffer, ColumnInfo> unselected)
    {
        if (unselected.isEmpty() )
            return EMPTY;
        return new VirtualCells(unselected, false);
    }
    
    public boolean isEmpty()
    {
        return payload.isEmpty();
    }

    public Map<ByteBuffer, ColumnInfo> keyOrConditions()
    {
        if (!isKeyOrFilter)
            return Collections.emptyMap();
        return payload;
    }

    public Map<ByteBuffer, ColumnInfo> unselected()
    {
        if (isKeyOrFilter)
            return Collections.emptyMap();
        return payload;
    }

    public boolean anyLiveUnselected(int nowInSec)
    {
        return ColumnInfo.anyAlive(unselected(), nowInSec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isKeyOrFilter, payload);
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof VirtualCells))
            return false;

        VirtualCells that = (VirtualCells) other;
        return this.isKeyOrFilter == that.isKeyOrFilter
                && this.payload == that.payload;
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

        assert isKeyOrFilter == another.isKeyOrFilter;

        Map<ByteBuffer, ColumnInfo> mergedPayload = merge(payload, another.payload);

        return VirtualCells.create(mergedPayload, isKeyOrFilter);
    }

    private static Map<ByteBuffer, ColumnInfo> merge(Map<ByteBuffer, ColumnInfo> left,
                                                     Map<ByteBuffer, ColumnInfo> right)
    {
        if (left.isEmpty())
            return right;
        if (right.isEmpty())
            return left;
        Map<ByteBuffer, ColumnInfo> mergedKeyOrConditions = new HashMap<>();
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
        return isKeyOrFilter && keyOrConditions().values().stream().anyMatch(c -> !c.isLive(nowInSeconds));
    }

    @Override
    public String toString()
    {
        return "VirtualCells [isKeyOrConditions=" + isKeyOrFilter + ", payLoad=" + payload + "]";
    }

    public int dataSize()
    {
        int size = 0;
        size += dataSize(payload);
        size += TypeSizes.sizeof(isKeyOrFilter);
        return size;
    }

    private static int dataSize(Map<ByteBuffer, ColumnInfo> payload)
    {
        int size = 0;
        for (Map.Entry<ByteBuffer, ColumnInfo> entry : payload.entrySet())
        {
            size += TypeSizes.sizeofWithLength(entry.getKey());
            size += entry.getValue().dataSize();
        }
        return size;
    }

    public void digest(MessageDigest digest)
    {
        digest(payload, digest);
    }

    private static void digest(Map<ByteBuffer, ColumnInfo> payload, MessageDigest digest)
    {
        if (payload.isEmpty())
            return;
        Object[] keys = payload.keySet().toArray();
        Arrays.sort(keys);
        for (Object key : keys)
            payload.get(key).digest(digest);
    }

    static class Serializer
    {
        public void serialize(VirtualCells virtualCells,
                              DataOutputPlus out,
                              SerializationHeader header) throws IOException
        {
            assert !virtualCells.isEmpty();
            out.writeBoolean(virtualCells.isKeyOrFilter);
            serializeRowVirtualCellsPayload(virtualCells.payload, header, out);
        }

        private void serializeRowVirtualCellsPayload(Map<ByteBuffer, ColumnInfo> payload,
                                                     SerializationHeader header,
                                                     DataOutputPlus out) throws IOException
        {
            out.writeInt(payload.size());
            for (Map.Entry<ByteBuffer, ColumnInfo> data : payload.entrySet())
            {
                ColumnInfo info = data.getValue();
                BytesType.instance.writeValue(data.getKey(), out);
                header.writeTTL(info.ttl(), out);
                header.writeTimestamp(info.timestamp(), out);
                header.writeLocalDeletionTime(info.localDeletionTime(), out);
            }
        }

        public VirtualCells deserialize(DataInputPlus in, SerializationHeader header, SerializationHelper helper) throws IOException
        {
            boolean isFilterOrCondition = in.readBoolean();
            Map<ByteBuffer, ColumnInfo> keyOrConditions = deserializeRowVirtualCellsPayload(header, in);
            return VirtualCells.create(keyOrConditions, isFilterOrCondition)
                               .filterDroppedColumns(helper.getBaseDroppedColumns());
        }

        private Map<ByteBuffer, ColumnInfo> deserializeRowVirtualCellsPayload(SerializationHeader header,
                                                                          DataInputPlus in) throws IOException
        {
            int size = in.readInt();
            if (size == 0)
                return Collections.emptyMap();
            Map<ByteBuffer, ColumnInfo> payload = new HashMap<>();
            for (int i = 0; i < size; i++)
            {
                ByteBuffer key = BytesType.instance.readValue(in, DatabaseDescriptor.getMaxValueSize());
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
                size += TypeSizes.sizeof(virtualCells.isKeyOrFilter);
                size += serializeRowVirtualCellsPayloadSize(virtualCells.payload, header);
            }
            return size;
        }

        private long serializeRowVirtualCellsPayloadSize(Map<ByteBuffer, ColumnInfo> payload,
                                                         SerializationHeader header)
        {
            long size = 0;
            size += TypeSizes.sizeof(payload.size());
            for (Map.Entry<ByteBuffer, ColumnInfo> data : payload.entrySet())
            {
                ColumnInfo info = data.getValue();
                size += BytesType.instance.writtenLength(data.getKey());
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
        for (Map.Entry<ByteBuffer, ColumnInfo> data : unselected().entrySet())
        {
            ColumnInfo info = data.getValue();
            max = max == null ? info : max.merge(info);
        }
        return max;
    }

    public ColumnInfo maxKeyOrConditionsDeadColumn(int nowInSec)
    {
        ColumnInfo max = null;
        for (Map.Entry<ByteBuffer, ColumnInfo> data : keyOrConditions().entrySet())
        {
            ColumnInfo info = data.getValue();
            if (!info.isLive(nowInSec))
                max = max == null ? info : max.merge(info); // greatest dead column
        }
        return max;
    }


    public VirtualCells filterDroppedColumns(Map<ByteBuffer, DroppedColumn> baseDroppedColumns)
    {
        if (baseDroppedColumns.isEmpty() || unselected().isEmpty())
            return this;
        // only unselected columns can be dropped in base
        Map<ByteBuffer, ColumnInfo> filteredUnselected = new HashMap<>();
        for (Map.Entry<ByteBuffer, ColumnInfo> entry : unselected().entrySet())
        {
            ByteBuffer name = entry.getKey();
            DroppedColumn dropped = baseDroppedColumns.get(name);
            if (dropped != null && entry.getValue().timestamp() <= dropped.droppedTime)
                continue;
            filteredUnselected.put(entry.getKey(), entry.getValue());
        }
        if (filteredUnselected.size() == unselected().size())
            return this;// not filtered
        return new VirtualCells(filteredUnselected, isKeyOrFilter);
    }
}