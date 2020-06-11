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
package org.apache.cassandra.index.sai.functional;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.SSTableIndexWriter;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexSSTableWriter;
import org.apache.cassandra.repair.RepairInfo;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionTest extends AbstractNodeLifecycleTest
{
    @Test
    public void testAntiCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String indexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        verifyIndexFiles(0, 0);

        // create 100 rows in 1 sstable
        int num = 100;
        for (int i = 0; i < num; i++)
            execute( "INSERT INTO %s (id, v1) VALUES (?, 0)", Integer.toString(i));
        flush();

        // verify 1 sstable index
        assertNumRows(num, "SELECT * FROM %%s WHERE v1 >= 0");
        verifyIndexFiles(1, 0);
        verifySSTableIndexes(indexName, 1);

        // split sstable into repaired and unrepaired
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        List<Range<Token>> ranges = Collections.singletonList(new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                                          DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes("30"))));
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.ANTICOMPACTION);
             Refs<SSTableReader> refs = Refs.ref(sstables))
        {

            UUID parentRepairSession = UUID.randomUUID();
            RepairInfo pendingRepairSession = RepairInfo.NONE;
            ActiveRepairService.instance.registerParentRepairSession(parentRepairSession,
                                                                     InetAddress.getByName("10.0.0.1"),
                                                                     Lists.newArrayList(cfs),
                                                                     ranges,
                                                                     true,
                                                                     1000,
                                                                     PreviewKind.NONE);
            CompactionManager.instance.performAnticompaction(cfs, ranges, refs, txn, pendingRepairSession, parentRepairSession);
        }

        // verify 2 sstable indexes
        assertNumRows(num, "SELECT * FROM %%s WHERE v1 >= 0");
        waitForAssert(() -> verifyIndexFiles(2, 0), WAIT_FOR_INDEX_FILE_CLEANUP);
        verifySSTableIndexes(indexName, 2);

        // index components are included after anti-compaction
        verifyIndexComponentsIncludedInSSTable(currentTable());
    }

    @Test
    public void testConcurrentQueryWithCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int num = 10;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
            flush();
        }

        TestWithConcurrentVerification compactionTest = new TestWithConcurrentVerification(() -> {
            for (int i = 0; i < 30; i++)
            {
                try
                {
                    assertNumRows(num, "SELECT id FROM %s WHERE v1>=0");
                    assertNumRows(num, "SELECT id FROM %s WHERE v2='0'");
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        }, () -> upgradeSSTables());

        compactionTest.start();

        verifySSTableIndexes(v1IndexName, num);
        verifySSTableIndexes(v2IndexName, num);
        assertValidationCount(0, 0);
    }

    @Test
    public void testAbortCompactionWithEarlyOpenSSTables() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));

        int sstables = 2;
        int num = 10;
        for (int i = 0; i < num; i++)
        {
            if (i == num / sstables)
                flush();

            execute("INSERT INTO %s (id, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        // make sure early open is triggered
        Injections.Counter earlyOpenCounter = Injections.newCounter("early_open_counter")
                                                                    .add(InvokePointBuilder.newInvokePoint().onClass(LifecycleTransaction.class).onMethod("checkpoint"))
                                                                    .build();

        // abort compaction
        String errMessage = "Injected failure!";
        Injection failSSTableCompaction = Injections.newCustom("fail_sstable_compaction")
                                                          .add(InvokePointBuilder.newInvokePoint().onClass(SSTableWriter.class).onMethod("prepareToCommit"))
                                                          .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote(errMessage)))
                                                          .build();

        try
        {
            Injections.inject(failSSTableCompaction, earlyOpenCounter);

            compact();
            fail("Expected compaction being interrupted");
        }
        catch (Throwable e)
        {
            while (e.getCause() != null)
                e = e.getCause();

            assertTrue(String.format("Expected %s, but got %s", errMessage, e.getMessage()), e.getMessage().contains(errMessage));
        }
        finally
        {
            earlyOpenCounter.disable();
            failSSTableCompaction.disable();
        }
        Assert.assertNotEquals(0, earlyOpenCounter.get());

        // verify indexes are working
        assertNumRows(num, "SELECT id FROM %%s WHERE v1=0");
        assertNumRows(num, "SELECT id FROM %%s WHERE v2='0'");
        verifySSTableIndexes(v1IndexName, sstables);
        verifySSTableIndexes(v2IndexName, sstables);
    }

    @Test
    public void testConcurrentIndexBuildWithCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        // prepare data into one sstable
        int sstables = 1;
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        Injections.Barrier compactionLatch =
                Injections.newBarrier("pause_compaction", 2, false)
                                .add(InvokePointBuilder.newInvokePoint().onClass(TrieIndexSSTableWriter.class).onMethod("append"))
                                .build();

        try
        {
            // stop in-progress compaction
            Injections.inject(compactionLatch);

            TestWithConcurrentVerification compactionTask = new TestWithConcurrentVerification(
                    () -> {
                        try
                        {
                            upgradeSSTables();
                            fail("Expected CompactionInterruptedException");
                        }
                        catch (Exception e)
                        {
                            assertTrue("Expected CompactionInterruptedException, but got " + e,
                                       Throwables.isCausedBy(e, CompactionInterruptedException.class));
                        }
                    },
                    () -> {
                        try
                        {
                            waitForAssert(() -> Assert.assertEquals(1, compactionLatch.getCount()), WAIT_FOR_COMPACTION_STARTED);

                            // build indexes on SSTables that will be compacted soon
                            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
                            createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
                            waitForIndexQueryable();

                            // continue in-progress compaction
                            compactionLatch.countDown();
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException(e);
                        }
                    }, -1 // run verification task once
            );

            compactionTask.start();
        }
        finally
        {
            compactionLatch.disable();
        }

        assertNumRows(num, "SELECT id FROM %%s WHERE v1>=0");
        assertNumRows(num, "SELECT id FROM %%s WHERE v2='0'");
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V1_COLUMN_IDENTIFIER), sstables);
        verifySSTableIndexes(IndexMetadata.generateDefaultIndexName(currentTable(), V2_COLUMN_IDENTIFIER), sstables);
    }

    @Test
    public void testConcurrentIndexDropWithCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));


        // Load data into a single SSTable...
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        assertNotEquals(0, getOpenIndexFiles(currentTable()));
        assertNotEquals(0, getDiskUsage(currentTable()));

        Injections.Barrier compactionLatch =
                Injections.newBarrier("pause_compaction_for_drop", 2, false)
                                .add(InvokePointBuilder.newInvokePoint().onClass(SSTableIndexWriter.class).onMethod("addRow"))
                                .build();
        try
        {
            // pause in-progress compaction
            Injections.inject(compactionLatch);

            TestWithConcurrentVerification compactionTask = new TestWithConcurrentVerification(
                    () -> upgradeSSTables(),
                    () -> {
                        try
                        {
                            waitForAssert(() -> Assert.assertEquals(1, compactionLatch.getCount()), WAIT_FOR_COMPACTION_STARTED);

                            // drop all indexes
                            dropIndex("DROP INDEX %s." + v1IndexName);
                            dropIndex("DROP INDEX %s." + v2IndexName);

                            // continue in-progress compaction
                            compactionLatch.countDown();
                        }
                        catch (Throwable e)
                        {
                            throw new RuntimeException(e);
                        }
                    }, -1 // run verification task once
            );

            compactionTask.start();
            waitForCompactionsFinished();
        }
        finally
        {
            compactionLatch.disable();
        }

        // verify index group metrics are cleared.
        assertEquals(0, getOpenIndexFiles(currentTable()));
        assertEquals(0, getDiskUsage(currentTable()));

        // verify indexes are dropped
        // verify indexes are dropped
        assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE v1>=0"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
        assertThatThrownBy(() -> executeNet("SELECT id FROM %s WHERE v2='0'"))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

}
