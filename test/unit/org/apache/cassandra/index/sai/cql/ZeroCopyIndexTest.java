/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.cql;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.cassandra.categories.NightlyOnly;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.SegmentBuilder;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;

@Category(NightlyOnly.class)
public class ZeroCopyIndexTest extends SAITester
{
    private static final Injections.Counter partitionsReadCounter =
            Injections.newCounter("partitions_read")
                            .add(InvokePointBuilder.newInvokePoint().onClass("org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher$ResultRetriever")
                                                 .onMethod("apply"))
                            .build();

    private static final Injection failIndexBuild = Injections.newCustom("fail_index_build")
                                                                    .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexBuilder.class).onMethod("indexSSTable"))
                                                                    .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                                    .build();

    private static final int rowIdsPerSegments = 3;
    private static final int num = 20;

    // key values in token order
    private static final List<String> keys = IntStream.range(0, num).mapToObj(String::valueOf).sorted((a, b) -> {
        DecoratedKey keyA = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose(a));
        DecoratedKey keyB = Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.decompose(b));
        return keyA.compareTo(keyB);
    }).collect(Collectors.toList());

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
        Injections.inject(partitionsReadCounter, failIndexBuild);
    }

    @Test
    public void testSkippingOnZeroCopiedIndexFiles() throws Throwable
    {
        // simulate partial zero-copy streaming on any pair of keys, and verify index query will not access partition
        // offset out of transferred range.
        prepareData();
        assertEquals(keys, fetchAllKeys());

        for (int startInclusive = 0; startInclusive < num - 1; startInclusive++)
        {
            for (int endInclusive = startInclusive; endInclusive < num; endInclusive++)
            {
                simulateZerocopyStreaming(startInclusive, endInclusive);

                // verify all keys from indexes
                long count = partitionsReadCounter.get();
                assertEquals(keys.subList(startInclusive, endInclusive + 1), fetchAllKeys());
                Assert.assertEquals(endInclusive - startInclusive + 1, partitionsReadCounter.get() - count);

                // verify partition restricted query
                for (int i = 0; i < keys.size(); i++)
                {
                    boolean included = i >= startInclusive && i <= endInclusive;
                    count = partitionsReadCounter.get();
                    assertEquals(included ? keys.get(i) : null, fetchByKey(keys.get(i)));
                    Assert.assertEquals(included ? 1 : 0, partitionsReadCounter.get() - count);
                }
            }
        }
    }

    private void prepareData() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForIndexQueryable();

        for (int i = 0; i < num; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        flush();

        // rewrite index files with multiple segments
        SegmentBuilder.updateLastValidSegmentRowId(rowIdsPerSegments - 1);

        upgradeSSTables();
    }

    private void simulateZerocopyStreaming(int firstKeyIdxInclusive, int lastKeyIdxInclusive) throws Exception
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(SchemaManager.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());

        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());
        // reset to no zero-copy, so that we can re-compute section
        sstable = mutateZerocopyMetadata(sstable, ZeroCopyMetadata.EMPTY);
        verifySSTableMinMaxKey(sstable, keys.get(0), keys.get(keys.size() - 1));

        // get the reset sstable
        sstable = mutateZerocopyMetadata(firstKeyIdxInclusive, lastKeyIdxInclusive, sstable);

        // verify sstable zero-copy metadata is updated
        verifySSTableMinMaxKey(sstable, keys.get(firstKeyIdxInclusive), keys.get(lastKeyIdxInclusive));
    }

    private void verifySSTableMinMaxKey(SSTableReader sstable, String first, String last)
    {
        if (sstable.getSSTableMetadata().zeroCopyMetadata.exists())
        {
            assertEquals(first, UTF8Type.instance.compose(sstable.getSSTableMetadata().zeroCopyMetadata.firstKey));
            assertEquals(last, UTF8Type.instance.compose(sstable.getSSTableMetadata().zeroCopyMetadata.lastKey));
        }

        assertEquals(first, UTF8Type.instance.compose(sstable.first.getKey()));
        assertEquals(last, UTF8Type.instance.compose(sstable.last.getKey()));
    }

    private SSTableReader mutateZerocopyMetadata(int firstKeyIdxInclusive, int lastKeyIdxInclusive, SSTableReader sstable)
    {
        long startOffset = sstable.getPosition(getKey(firstKeyIdxInclusive), SSTableReader.Operator.EQ).position;
        RowIndexEntry rightEntry = sstable.getPosition(getKey(lastKeyIdxInclusive), SSTableReader.Operator.GT);
        long endOffset = rightEntry == null ? sstable.uncompressedLength() : rightEntry.position;

        int chunkSize = getChunkSize(sstable);
        long estimatedKeys = keys.size(); // not used
        ByteBuffer firstKey = getKeyValue(firstKeyIdxInclusive);
        ByteBuffer lastKey = getKeyValue(lastKeyIdxInclusive);

        ZeroCopyMetadata zeroCopyMetadata = new ZeroCopyMetadata(startOffset, endOffset, chunkSize, estimatedKeys, firstKey, lastKey);
        return mutateZerocopyMetadata(sstable, zeroCopyMetadata);
    }

    private int getChunkSize(SSTableReader sstable)
    {
        int chunkSize = 0;
        if (sstable.compression)
        {
            chunkSize = sstable.getCompressionMetadata().chunkLength();
        }
        else if (sstable.descriptor.filenameFor(Component.CRC).exists())
        {
            File crcFile = sstable.descriptor.filenameFor(Component.CRC);
            try (RandomAccessReader crcReader = RandomAccessReader.open(crcFile))
            {
                chunkSize = crcReader.readInt();
            }
            catch (IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        return chunkSize;
    }

    private SSTableReader mutateZerocopyMetadata(SSTableReader sstable, ZeroCopyMetadata zeroCopyMetadata)
    {
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateZeroCopyMetadata(sstable.descriptor, zeroCopyMetadata);
            sstable.reloadSSTableMetadata();

            // need to reopen sstable to have new zeroCopyMetadata applied
            return reloadSSTableAndIndexes(sstable);
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    private SSTableReader reloadSSTableAndIndexes(SSTableReader sstable)
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(sstable.metadata());
        Objects.requireNonNull(StorageAttachedIndexGroup.getIndexGroup(cfs)).sstableContextManager().clear();
        StorageAttachedIndex sai = (StorageAttachedIndex) Iterables.getOnlyElement(cfs.indexManager.listIndexes());
        sai.getContext().onSSTableChanged(Collections.singleton(sstable), Collections.emptyList(), false);

        cfs.clearUnsafe();
        ColumnFamilyStore.loadNewSSTables(KEYSPACE, sstable.metadata().name);

        return Iterables.getOnlyElement(cfs.getLiveSSTables());
    }

    private DecoratedKey getKey(int idx)
    {
        return Murmur3Partitioner.instance.decorateKey(getKeyValue(idx));
    }

    private ByteBuffer getKeyValue(int idx)
    {
        return UTF8Type.instance.decompose(keys.get(idx));
    }

    private List<String> fetchAllKeys() throws Throwable
    {
        List<Row> rows = executeNet("SELECT id1 FROM %s WHERE v1>=0").all();
        return rows.stream().map(row -> row.getString(0)).collect(Collectors.toList());
    }

    private String fetchByKey(String key) throws Throwable
    {
        List<Row> rows = executeNet(String.format("SELECT id1 FROM %%s WHERE id1='%s' AND v1>=0", key)).all();
        return rows.isEmpty() ? null : Iterables.getOnlyElement(rows).getString(0);
    }
}
