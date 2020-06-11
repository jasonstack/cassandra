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

import java.io.File;
import java.util.ArrayList;
import java.util.Objects;

import com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.cassandra.categories.NightlyOnly;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Expression;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaManager;

@Category(NightlyOnly.class)
public class DropTableTest extends AbstractNodeLifecycleTest
{
    @Test
    public void testDropTableLifecycle() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int rows = 100;
        for (int j = 0; j < rows; j++)
        {
            execute("INSERT INTO %s (id, v1, v2) VALUES (?, 1 , '1')", Integer.toString(j));
        }
        flush();

        verifyIndexComponentsIncludedInSSTable(currentTable());

        ColumnFamilyStore cfs = Objects.requireNonNull(SchemaManager.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(currentTable());
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        ArrayList<String> files = new ArrayList<>();
        for (Component component : sstable.components())
        {
            File file = sstable.descriptor.filenameFor(component);
            if (file.exists())
                files.add(file.getPath());
        }

        Injection failUnregisterComponents = Injections.newCustom("fail_unregister_components")
                                                            .add(InvokePointBuilder.newInvokePoint().onClass(SSTable.class).onMethod("unregisterComponents"))
                                                            .add(ActionBuilder.newActionBuilder().actions().doThrow(RuntimeException.class, Expression.quote("Injected failure!")))
                                                            .build();
        assertAllFileExists(files);

        Injections.inject(failUnregisterComponents);

        // drop table, on disk files should be removed. `SSTable#unregisterComponents` should not be call
        dropTable("DROP TABLE %s");

        assertAllFileRemoved(files);
    }
}
