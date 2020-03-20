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

package org.apache.cassandra.cql3.validation.operations;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class AutoExpansionNTSTest extends CQLTester
{

    @Test
    public void testCreateAlterNetworkTopologyWithDefaults() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.4");
        metadata.updateHostId(UUID.randomUUID(), local);
        metadata.updateNormalToken(new OrderPreservingPartitioner.StringToken("A"), local);
        metadata.updateHostId(UUID.randomUUID(), remote);
        metadata.updateNormalToken(new OrderPreservingPartitioner.StringToken("B"), remote);

        // With two datacenters we should respect anything passed in as a manual override
        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1, '" + DATA_CENTER_REMOTE + "': 3}");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER, "1", DATA_CENTER_REMOTE, "3")));

        // Should be able to remove data centers
        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy', '" + DATA_CENTER + "' : 0, '" + DATA_CENTER_REMOTE + "': 3 }");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER_REMOTE, "3")));

        // The auto-expansion should not change existing replication counts; do not let the user shoot themselves in the foot
        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 } AND durable_writes=True");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER, "1", DATA_CENTER_REMOTE, "3")));

        // The keyspace should be fully functional
        execute("USE " + ks1);

        assertInvalidThrow(ConfigurationException.class, "CREATE TABLE tbl1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }");

        execute("CREATE TABLE tbl1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }");

        assertRows(execute("SELECT table_name, compaction FROM system_schema.tables WHERE keyspace_name='" + ks1 + "'"),
                   row("tbl1", map("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                                   "min_threshold", "7",
                                   "max_threshold", "32")));
    }

    @Test
    public void testCreateSimpleAlterNTSDefaults() throws Throwable
    {
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.4");
        metadata.updateHostId(UUID.randomUUID(), local);
        metadata.updateNormalToken(new OrderPreservingPartitioner.StringToken("A"), local);
        metadata.updateHostId(UUID.randomUUID(), remote);
        metadata.updateNormalToken(new OrderPreservingPartitioner.StringToken("B"), remote);

        // Let's create a keyspace first with SimpleStrategy
        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2}");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "2")));

        // Now we should be able to ALTER to NetworkTopologyStrategy directly from SimpleStrategy without supplying replication_factor
        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy'}");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER, "2", DATA_CENTER_REMOTE, "2")));

        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3}");
        schemaChange("ALTER KEYSPACE " + ks1 + " WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor': 2}");

        assertRowsIgnoringOrderAndExtra(execute("SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces"),
                                        row(KEYSPACE, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(KEYSPACE_PER_TEST, true, map("class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1")),
                                        row(ks1, true, map("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", DATA_CENTER, "2", DATA_CENTER_REMOTE, "2")));
    }
}
