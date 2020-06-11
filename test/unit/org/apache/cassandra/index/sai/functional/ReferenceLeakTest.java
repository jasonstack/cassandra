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

import java.util.Objects;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.cassandra.categories.NightlyOnly;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NightlyOnly.class)
public class ReferenceLeakTest extends AbstractNodeLifecycleTest
{
    @Test
    public void testQueryReleasesReference() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForIndexQueryable();

        int num = 10;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id, v1) VALUES ('" + i + "', 0)");
        }
        flush();

        Injection doubleReleaseReference =
                Injections.newCustom("force_release_reference")
                                .add(InvokePointBuilder.newInvokePoint().atEntry().onClass(SSTableIndex.class).onMethod("release"))
                                // force sstable ref to be released and close file handler when read request completes to detect any outstanding disk access.
                                .add(ActionBuilder.newActionBuilder().actions().doAction(Expression.expr("$this.references.decrementAndGet()")))
                                // force chunk cache cleanup, so query will reload on disk data.
                                .add(ActionBuilder.newActionBuilder().actions().doAction(Expression.expr("org.apache.cassandra.cache.ChunkCache.instance.enable").args(true)))
                                .build();

        try
        {
            // force sstable index to release the SSTable and close index searches when query releases reference.
            Injections.inject(doubleReleaseReference);

            // verify current query is executed properly without closed file exception
            assertNumRows(num, "SELECT id FROM %%s WHERE v1>=0");

            // verify sstable-index is released
            Index index = Iterables.getOnlyElement(Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).indexManager.listIndexes());
            StorageAttachedIndex sai = (StorageAttachedIndex) index;
            SSTableIndex sstableIndex = Iterables.getOnlyElement(sai.getContext().getView().getIndexes());
            assertTrue(sstableIndex.isReleased());
        }
        finally
        {
            doubleReleaseReference.disable();
        }
    }

    /**
     * This test was created at a time when SSTableReaderTidier still had a reference to a
     * Tracker instance, and that instance was a critical link in a circular reference
     * on {@link SSTableContext}. So, while this tests isn't strictly necessary, it will
     * notify us if some other circular reference around compaction occurs in the future.
     */
    @Test
    public void testStrongRefLeak() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, v1, v2) VALUES ('1', 0, '0')");
        flush();
        execute("INSERT INTO %s (id, v1, v2) VALUES ('2', 1, '1')");
        flush();
        execute("INSERT INTO %s (id, v1, v2) VALUES ('3', 2, '2')");
        flush();

        // to simulate that Ref.Visitor is run before SSTableContextManager releases SSTableContexts but after CFS releases SSTables.
        Injections.Barrier pauseNotification = Injections.newBarrier("pause_notification", 2, false)
                                                                     .add(InvokePointBuilder.newInvokePoint().onClass(StorageAttachedIndexGroup.class).onMethod("onSSTableChanged"))
                                                                     .build();

        Injections.inject(pauseNotification);

        // no strong ref leak before compaction
        verifyCyclicRefs(false);

        // compaction sstables but pause on StorageAttachedIndexGroup#handleNotification
        startCompaction();

        // verify that old SSTables are released from CFS, but SSTableContexts are not yet released
        waitForAssert(() -> Assert.assertEquals(1, pauseNotification.getCount()), "wait for compaction paused");

        Keyspace ks = Objects.requireNonNull(SchemaManager.instance.getKeyspaceInstance(KEYSPACE));
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(currentTable());
        assertEquals(1, cfs.getLiveSSTables().size());

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertEquals(3, Objects.requireNonNull(group).sstableContextManager().size());

        // found strong ref leak during compaction
        verifyCyclicRefs(false);

        // allow compaction to proceed
        pauseNotification.countDown();
        waitForCompactionsFinished();

        // verify that SSTableContexts are released
        verifySSTableIndexes(v1IndexName, 1);
        verifySSTableIndexes(v2IndexName, 1);

        // no strong ref leak after compaction
        verifyCyclicRefs(false);
    }

    void verifyCyclicRefs(boolean foundCyclicRefs) throws Throwable
    {
        Ref.Visitor visitor = new Ref.Visitor();
        visitor.run();
        assertEquals(foundCyclicRefs, !visitor.previousCyclicRefs.isEmpty());
    }
}
