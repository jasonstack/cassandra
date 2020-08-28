/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.cql.types.collections.maps;

import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport;
import org.apache.cassandra.index.sai.cql.types.collections.CollectionDataSet;

@RunWith(Parameterized.class)
public class MapKeysFrozenCollectionTest extends IndexingTypeSupport
{
    @Parameterized.Parameters(name = "dataset={0},wide={1},scenario={2}")
    public static Collection<Object[]> generateParameters()
    {
        DataSet<Map<Integer, Integer>> frozen = new CollectionDataSet.FrozenMapValuesDataSet<>(random, new DataSet.IntDataSet(random));
        return generateParameters(new CollectionDataSet.MapKeysDataSet<>(random, frozen));
    }

    public MapKeysFrozenCollectionTest(DataSet<?> dataset, boolean widePartitions, Scenario scenario)
    {
        super(dataset, widePartitions, scenario);
    }

    @Test
    public void test() throws Throwable
    {
        runIndexQueryScenarios();
    }
}
