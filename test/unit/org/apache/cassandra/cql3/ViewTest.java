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

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

public class ViewTest extends CQLTester
{
    ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }


    @Test
    public void testPartialDelete() throws Throwable
    {
        boolean flush = true;
        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        createTable("CREATE TABLE %s (k int, c int, a int, b int, PRIMARY KEY (k, c))");
        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT k,c FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");
        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 10 SET b=1 WHERE k=1 AND c=1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT * from %s"), row(1, 1, null, 1));
        assertRows(execute("SELECT * from mv"), row(1, 1));
        updateView("DELETE b FROM %s USING TIMESTAMP 11 WHERE k=1 AND c=1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));
        updateView("UPDATE %s USING TIMESTAMP 1 SET a=1 WHERE k=1 AND c=1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT * from %s"), row(1, 1, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1));
    }

    @Test
    public void testUpdateColumnInViewPKWithTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(true);
    }

    @Test
    public void testUpdateColumnInViewPKWithTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(false);
    }

    private void testUpdateColumnInViewPKWithTTL(boolean flush) throws Throwable
    {
        // CASSANDRA-13657 if base column used in view pk is ttled, then view row is considered dead
        createTable("create table %s (k int primary key, a int, b int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s SET a = 1 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 1, null));
        assertRows(execute("SELECT * from mv"), row(1, 1, null));

        updateView("DELETE a FROM %s WHERE k = 1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));

        updateView("INSERT INTO %s (k) VALUES (1);");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from mv"));

        updateView("UPDATE %s USING TTL 5 SET a = 10 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 10, null));
        assertRows(execute("SELECT * from mv"), row(10, 1, null));

        updateView("UPDATE %s SET b = 100 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, 10, 100));
        assertRows(execute("SELECT * from mv"), row(10, 1, 100));

        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

        // 'a' is TTL of 5 and removed.
        assertRows(execute("SELECT * from %s"), row(1, null, 100));
        assertEmpty(execute("SELECT * from mv"));
        assertEmpty(execute("SELECT * from mv WHERE k = ? AND a = ?", 1, 10));

        updateView("DELETE b FROM %s WHERE k=1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from mv"));

        updateView("DELETE FROM %s WHERE k=1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from mv"));
    }

    @Test
    public void testUpdateColumnNotInViewWithFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUpdateColumnNotInView(true);
    }

    @Test
    public void testUpdateColumnNotInViewWithoutFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUpdateColumnNotInView(false);
    }

    private void testUpdateColumnNotInView(boolean flush) throws Throwable
    {
        // CASSANDRA-13127: if base column not selected in view are alive, then pk of view row should be alive
        createTable("create table %s (p int, c int, v1 int, v2 int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM %%s WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 0 SET v1 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, 1, null));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v1 FROM %s USING TIMESTAMP 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));

        // shadowed by tombstone
        updateView("UPDATE %s USING TIMESTAMP 1 SET v1 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s USING TIMESTAMP 2 SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v1 FROM %s USING TIMESTAMP 3 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateView("DELETE v2 FROM %s USING TIMESTAMP 4 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertEmpty(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s USING TTL 3 SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));

        updateView("UPDATE %s SET v2 = 1 WHERE p = 0 AND c = 0");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0), row(0, 0, null, 1));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        // drop unselected base column, unselected metadata should be removed, thus view row is dead
        updateView("ALTER TABLE %s DROP v2");
        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0));
        assertRowsIgnoringOrder(execute("SELECT * from %s"));
        assertRowsIgnoringOrder(execute("SELECT * from mv"));
    }

    @Test
    public void testPartialUpdateWithUnselectedCollectionsWithFlush() throws Throwable
    {
        testPartialUpdateWithUnselectedCollections(true); 
    }

    @Test
    public void testPartialUpdateWithUnselectedCollectionsWithoutFlush() throws Throwable
    {
        testPartialUpdateWithUnselectedCollections(false);
    }

    public void testPartialUpdateWithUnselectedCollections(boolean flush) throws Throwable
    {
        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        createTable("CREATE TABLE %s (k int, c int, a int, b int, l list<int>, s set<int>, m map<int,int>, PRIMARY KEY (k, c))");
        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");
        Keyspace ks = Keyspace.open(keyspace());
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("UPDATE %s SET l=l+[1,2,3] WHERE k = 1 AND c = 1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateView("UPDATE %s SET l=l-[1,2] WHERE k = 1 AND c = 1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT * from mv"), row(1, 1, null, null));

        updateView("UPDATE %s SET b=3 WHERE k=1 AND c=1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT * from mv"), row(1, 1, null, 3));

        updateView("UPDATE %s SET b=null, l=l-[3], s=s-{3} WHERE k = 1 AND c = 1");
        if (flush)
        {
            FBUtilities.waitOnFutures(ks.flush());
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
        }
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"));
        assertRowsIgnoringOrder(execute("SELECT * from mv"));

        updateView("UPDATE %s SET m=m+{3:3}, l=l-[1], s=s-{2} WHERE k = 1 AND c = 1");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"), row(1, 1, null, null));
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 1, null, null));

        executeNet(protocolVersion, "ALTER TABLE %s DROP m");
        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s WHERE k = 1 AND c = 1"));
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE k = 1 AND c = 1"));
        assertRowsIgnoringOrder(execute("SELECT k,c,a,b from %s"));
        assertRowsIgnoringOrder(execute("SELECT * from mv"));
    }

    @Test
    public void testViewTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13127
        viewTTLTest(true);
    }

    @Test
    public void testViewTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13127
        viewTTLTest(false);
    }

    private void viewTTLTest(boolean flush) throws Throwable
    {
        // CASSANDRA-13127 not ttled unselected column in base should keep view row alive
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                "CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM %%s WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        updateView("INSERT INTO %s (p, c) VALUES (0, 0) USING TTL 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        updateView("UPDATE %s USING TTL 1000 SET v = 0 WHERE p = 0 and c = 0;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        Thread.sleep(3000);

        UntypedResultSet.Row row = execute("SELECT v, ttl(v) from %s WHERE c = ? AND p = ?", 0, 0).one();
        assertTrue("row should have value of 0", row.getInt("v") == 0);
        assertTrue("row should have ttl less than 1000", row.getInt("ttl(v)") < 1000);
        assertRowsIgnoringOrder(execute("SELECT * from mv WHERE c = ? AND p = ?", 0, 0), row(0, 0));

    }

    @Test
    public void testBaseTTLWithSameTimestampTest() throws Throwable
    {
        // CASSANDRA-13127 when liveness timestamp tie, greate localDeletionTime should win if both are expiring.
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) using timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        // reversed order
        execute("truncate %s;");

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush());

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

    }

    @Test
    public void testCommutativeRowDeletionFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(true);
    }

    @Test
    public void testCommutativeRowDeletionWithoutFlush() throws Throwable
    {
        // CASSANDRA-13409
        testCommutativeRowDeletion(false);
    }

    private void testCommutativeRowDeletion(boolean flush)
            throws Throwable
    {
        // CASSANDRA-13409 new update should not resurrect previous deleted data in view
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable-1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable-2
        updateView("Delete from %s using timestamp 2 where p = 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"));
        // sstable-3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
        // sstable-4
        updateView("UPdate %s using timestamp 4 set v1 = 2 where p = 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, null, null));
        // sstable-5
        updateView("UPdate %s using timestamp 5 set v1 = 1 where p = 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 4, 5;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
            List<String> sstables = cfs.getLiveSSTables().stream()
                    .sorted((s1, s2) -> s1.descriptor.generation - s2.descriptor.generation)
                    .map(s -> s.getFilename())
                    .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(3), sstables.get(4)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
        }

        // regular tombstone should be retained after compaction
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(true);
    }

    @Test
    public void testUpdateWithColumnTimestampBiggerThanPkWithoutFlush() throws Throwable
    {
        // CASSANDRA-11500
        testUpdateWithColumnTimestampBiggerThanPk(false);
    }

    public void testUpdateWithColumnTimestampBiggerThanPk(boolean flush) throws Throwable
    {
        // CASSANDRA-11500 able to shadow old view row with column ts greater tahn pk's ts and re-insert the view row
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int);");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();
        updateView("DELETE FROM %s USING TIMESTAMP 0 WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // sstable-1, Set initial values TS=1
        updateView("INSERT INTO %s(k, a, b) VALUES (1, 1, 1) USING TIMESTAMP 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 1));
        updateView("UPDATE %s USING TIMESTAMP 10 SET b = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        updateView("UPDATE %s USING TIMESTAMP 2 SET a = 2 WHERE k = 1;");
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 2, 2));
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 2, 2));
        updateView("UPDATE %s USING TIMESTAMP 3 SET a = 1 WHERE k = 1;");
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT k,a,b from mv"), row(1, 1, 2));
        assertRowsIgnoringOrder(execute("SELECT k,a,b from %s"), row(1, 1, 2));
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(true);
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithoutFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(false);
    }

    private void testCellTombstoneAndShadowableTombstones(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable 1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable 2
        updateView("UPdate %s using timestamp 2 set v2 = null where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3),
                                row(null, null));
        // sstable 3
        updateView("UPdate %s using timestamp 3 set v1 = 2 where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, null, null));
        // sstable 4
        updateView("UPdate %s using timestamp 4 set v1 = 1 where p = 3");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 3;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
            List<String> sstables = cfs.getLiveSSTables().stream()
                    .sorted((s1, s2) -> s1.descriptor.generation - s2.descriptor.generation)
                    .map(s -> s.getFilename())
                    .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(2), sstables.get(3)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
        }
        // cell-tombstone in sstable 4 is not compacted away, because the shadowable tombstone is shadowed by new row.
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
    }

    @Test
    public void complexTimestampDeletionTestWithFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(true);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(true);
    }

    @Test
    public void complexTimestampDeletionTestWithoutFlush() throws Throwable
    {
        complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(false);
        complexTimestampWithbasePKColumnsInViewPKDeletionTest(false);
    }

    private void complexTimestampWithbasePKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key(p1, p2))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv2",
                "create materialized view %s as select * from %%s where p1 is not null and p2 is not null primary key (p2, p1);");
        ks.getColumnFamilyStore("mv2").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p1, p2, v1, v2) values (1, 2, 3, 4) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, v2, WRITETIME(v2) from mv2 WHERE p1 = ? AND p2 = ?", 1, 2),
                                row(3, 4, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p1 = 1 and p2 = 2;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv2"));
        // insert PK with TS=3
        updateView("Insert into %s (p1, p2) values (1, 2) using timestamp 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));

        ks.getColumnFamilyStore("mv2").forceMajorCompaction();
        assertRowsIgnoringOrder(execute("SELECT * from mv2"), row(2, 1, null, null));
    }

    public void complexTimestampWithbaseNonPKColumnsInViewPKDeletionTest(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(5, 1L));
        // remove row/mv TS=2
        updateView("Delete from %s using timestamp 2 where p = 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // view are empty
        assertRowsIgnoringOrder(execute("SELECT * from mv"));
        // insert PK with TS=3
        updateView("Insert into %s (p, v1) values (3, 1) using timestamp 3;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        updateView("Insert into %s (p, v1, v2) values (3, 1, 5) using timestamp 2;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        // deleted column in MV remained dead
        assertRowsIgnoringOrder(execute("SELECT * from mv"), row(1, 3, null));

        // insert values TS=2, it should be considered dead due to previous tombstone
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET v2 = ? WHERE p = ?", 4, 3);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));

        ks.getColumnFamilyStore("mv").forceMajorCompaction();
        assertRows(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, 4, 3L));
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    private void updateView(String query, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getPendingTasks() == 0
                 && ((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getActiveCount() == 0))
        {
            Thread.sleep(1);
        }
    }

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW " + KEYSPACE + ".view_does_not_exist");
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist");

        execute("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".view_does_not_exist");
        execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist");
    }

    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from view1").size());
    }


    @Test
    public void createMvWithUnrestrictedPKParts() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from view1").size());
    }

    @Test
    public void testPrimaryKeyIsNotNull() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must include both when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        dropTable("DROP TABLE %s");

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY(k, asciival))");
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Can omit "k IS NOT NULL" because we have a sinlge partition key
        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
    }

    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "sval text static, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)");
            Assert.fail("Use of static column in a MV primary key should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)");
            Assert.fail("Explicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");
            Assert.fail("Implicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

        Assert.assertEquals(2, execute("select * from %s").size());

        assertRows(execute("SELECT sval from %s"), row("bar99"), row("bar99"));

        Assert.assertEquals(2, execute("select * from mv_static").size());

        assertInvalid("SELECT sval from mv_static");
    }


    @Test
    public void testOldTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_tstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv_tstest").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"), row(0));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(1));
    }

    @Test
    public void testRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_rctstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(1, 0, 1));

        updateView("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(2, 0, 2));
    }

    @Test
    public void testCountersTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "count counter)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_counter", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)");
            Assert.fail("MV on counter should fail");
        }
        catch (InvalidQueryException e)
        {
        }
    }

    @Test
    public void testDurationsTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "result duration)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_duration", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)");
            Assert.fail("MV on duration should fail");
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("Cannot use Duration column 'result' in PRIMARY KEY of materialized view", e.getMessage());
        }
    }

    @Test
    public void testFrozenCollectionsWithComplexInnerType() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int, " +
                "intval int, " +
                "listval frozen<list<tuple<text,text>>>, " +
                "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))",
                   0,
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));

        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))",
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
    }

    @Test
    public void complexTimestampUpdateTestWithFlush() throws Throwable
    {
        complexTimestampUpdateTest(true);
    }

    @Test
    public void complexTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexTimestampUpdateTest(false);
    }

    public void complexTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        //Set initial values TS=0, leaving e null and verify view
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        //update c's timestamp TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0));

        if(flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }


        //change c's value back to 1 with TS=4, check we can see d
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }

        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));


            //Add e value @ TS=1
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=3
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));


        //Tombstone c
        executeNet(protocolVersion, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRows(execute("SELECT d from mv"));

        //Add back without D
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));


        //New partition
        // insert a row with timestamp 0
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet(protocolVersion, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, 0, 0)
        );


    }

    @Test
    public void testBuilderWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "PRIMARY KEY (k, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());


        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval) VALUES (?, ?, ?)", 0, i, 0);

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");


        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv"))
            Thread.sleep(1000);

        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));
        assertRows(execute("SELECT count(*) from mv WHERE intval = ?", 0), row(1024L));
    }

    @Test
    public void testRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "textval2 text, " +
                    "PRIMARY KEY((k, asciival), bigintval, textval1)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_test1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1,textval2)VALUES(?,?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i, "baz");

        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());

        Assert.assertEquals(100, execute("select * from mv_test1").size());

        //Check the builder works
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test2").size());

        createView("mv_test3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test3"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test3").size());
        Assert.assertEquals(100, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(50, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());
    }


    @Test
    public void testRangeTombstone2() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from mv").size());
    }

    @Test
    public void testRangeTombstone3() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval >= ?", 0, "foo", 0L);

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from mv").size());
    }

    @Test
    public void testCompoundPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        TableMetadata metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnMetadata def : new HashSet<>(metadata.columns()))
        {
            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ("
                               + def.name + ", k" + (def.name.toString().equals("asciival") ? "" : ", asciival") + ")";
                createView("mv1_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + " PRIMARY KEY ("
                               + def.name + ", asciival" + (def.name.toString().equals("k") ? "" : ", k") + ")";
                createView("mv2_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                Assert.fail("Should fail on duplicate name");
            }
            catch (Exception e)
            {
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), nonexistentcolumn)";
                createView("mv3_" + def.name, query);
                Assert.fail("Should fail with unknown base column");
            }
            catch (InvalidQueryException e)
            {
            }
        }

        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "123123123123");
        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 123123123123L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L), row(0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0), row("ascii text"));


        //UPDATE BASE
        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "1");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 1L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 1L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0), row("ascii text"));


        //test truncate also truncates all MV
        updateView("TRUNCATE %s");

        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0));
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "listval list<int>, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))", 0, 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 0), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 1, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 1), row(1, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 1), row(1, list(1, 2, 3)));
    }

    @Test
    public void testFrozenCollectionsWithComplicatedInnerType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, intval int,  listval frozen<list<tuple<text,text>>>, PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))",
                   0,
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");

        // verify input
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));

        // update listval with the same value and it will be compared in view generator
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))",
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        // verify result
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
    }

    @Test
    public void testUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testIgnoreUpdate() throws Throwable
    {
        // regression test for CASSANDRA-10614

        createTable("CREATE TABLE %s (" +
                    "a int, " +
                    "b int, " +
                    "c int, " +
                    "d int, " +
                    "PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

        updateView("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

        // Note: errors here may result in the test hanging when the memtables are flushed as part of the table drop,
        // because empty rows in the memtable will cause the flush to fail.  This will result in a test timeout that
        // should not be ignored.
        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                "INSERT INTO " + table + " (a, b, c, d) VALUES (?, ?, ?, ?); " + // should be accepted
                "UPDATE " + table + " SET d = ? WHERE a = ? AND b = ?; " + // should be accepted
                "APPLY BATCH",
                0, 0, 0, 0,
                1, 0, 1);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 1), row(0, 1, null));

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore("mv");
        cfs.forceBlockingFlush();
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void ttlTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        List<Row> results = executeNet(protocolVersion, "SELECT d FROM mv WHERE c = 2 AND a = 1 AND b = 1").all();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue("There should be a null result given back due to ttl expiry", results.get(0).isNull(0));
    }

    @Test
    public void ttlExpirationTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void rowDeletionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        String table = keyspace() + "." + currentTable();
        updateView("DELETE FROM " + table + " USING TIMESTAMP 6 WHERE a = 1 AND b = 1;");
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP 3", 1, 1, 1, 1);
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void conflictingTimestampTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        for (int i = 0; i < 50; i++)
        {
            updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i);
        }

        ResultSet mvRows = executeNet(protocolVersion, "SELECT c FROM mv");
        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM %s").all();
        Assert.assertEquals("There should be exactly one row in base", 1, rows.size());
        int expected = rows.get(0).getInt("c");
        assertRowsNet(protocolVersion, mvRows, row(expected));
    }

    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b, c))" +
                    "WITH CLUSTERING ORDER BY (b ASC, c DESC)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c) WITH CLUSTERING ORDER BY (b DESC)");
        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c ASC)");
        createView("mv3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");
        createView("mv4", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c DESC)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 2, 2);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv2");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT b FROM mv3");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv4");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));
    }

    @Test
    public void testMultipleDeletes() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 2);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 3);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(1, 1),
                      row(1, 2),
                      row(1, 3));

        updateView(String.format("BEGIN UNLOGGED BATCH " +
                   "DELETE FROM %s WHERE a = 1 AND b > 1 AND b < 3;" +
                   "DELETE FROM %s WHERE a = 1;" +
                   "APPLY BATCH", currentTable(), currentTable()));

        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows);
    }

    @Test
    public void testPrimaryKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }

    @Test
    public void testPartitionKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY ((a, b)))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }

    @Test
    public void testDeleteSingleColumnInViewClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (a, d, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testDeleteSingleColumnInViewPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (d, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testCollectionInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c map<int, text>," +
                    "PRIMARY KEY (a))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mvmap", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, map(1, "1"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 1);
        assertRowsNet(protocolVersion, mvRows, row(1, 1));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, map(0, "0"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));
    }

    @Test
    public void testMultipleNonPrimaryKeysInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "e int," +
                    "PRIMARY KEY ((a, b), c))");

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((d, a), b, e, c)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((a, b), c, d, e)");
            Assert.fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }

    }

    @Test
    public void testNullInClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, v1 text, v2 text, PRIMARY KEY (id1, id2))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT id1, v1, id2, v2" +
                   "  FROM %%s" +
                   "  WHERE id1 IS NOT NULL AND v1 IS NOT NULL AND id2 IS NOT NULL" +
                   "  PRIMARY KEY (id1, v1, id2)" +
                   "  WITH CLUSTERING ORDER BY (v1 DESC, id2 ASC)");

        execute("INSERT INTO %s (id1, id2, v1, v2) VALUES (?, ?, ?, ?)", 0, 1, "foo", "bar");

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, "foo", "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(0, "foo", 1, "bar"));

        executeNet(protocolVersion, "UPDATE %s SET v1=? WHERE id1=? AND id2=?", null, 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));

        executeNet(protocolVersion, "UPDATE %s SET v2=? WHERE id1=? AND id2=?", "rab", 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "rab"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));
    }

    @Test
    public void testReservedKeywordsInMV() throws Throwable
    {
        createTable("CREATE TABLE %s (\"token\" int PRIMARY KEY, \"keyspace\" int)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT \"keyspace\", \"token\"" +
                   "  FROM %%s" +
                   "  WHERE \"keyspace\" IS NOT NULL AND \"token\" IS NOT NULL" +
                   "  PRIMARY KEY (\"keyspace\", \"token\")");

        execute("INSERT INTO %s (\"token\", \"keyspace\") VALUES (?, ?)", 0, 1);

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(1, 0));
    }

    public void testCreateMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "c int, " +
                "val int) WITH default_time_to_live = 60");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must NOT include "default_time_to_live" for Materialized View creation
        try
        {
            createView("mv_ttl1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 30");
            Assert.fail("Should fail if TTL is provided for materialized view");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testAlterMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        createView("mv_ttl2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        // Must NOT include "default_time_to_live" on alter Materialized View
        try
        {
            executeNet(protocolVersion, "ALTER MATERIALIZED VIEW %s WITH default_time_to_live = 30");
            Assert.fail("Should fail if TTL is provided while altering materialized view");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testViewBuilderResume() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        CompactionManager.instance.setCoreCompactorThreads(1);
        CompactionManager.instance.setMaximumCompactorThreads(1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        cfs.enableAutoCompaction();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(cfs);

        //Force a second MV on the same base table, which will restart the first MV builder...
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT val, k, c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");


        //Compact the base table
        FBUtilities.waitOnFutures(futures);

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertRows(execute("SELECT count(*) FROM mv_test"), row(1024L));
    }
}
