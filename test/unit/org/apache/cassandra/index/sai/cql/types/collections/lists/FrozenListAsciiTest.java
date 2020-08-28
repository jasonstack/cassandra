package org.apache.cassandra.index.sai.cql.types.collections.lists;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.cql.types.DataSet;
import org.apache.cassandra.index.sai.cql.types.IndexingTypeSupport;
import org.apache.cassandra.index.sai.cql.types.collections.CollectionDataSet;

@RunWith(Parameterized.class)
public class FrozenListAsciiTest extends IndexingTypeSupport
{
    @Parameterized.Parameters(name = "dataset={0},wide={1},scenario={2}")
    public static Collection<Object[]> generateParameters()
    {
        return generateParameters(new CollectionDataSet.FrozenListDataSet<>(random, new DataSet.AsciiDataSet(random)));
    }

    public FrozenListAsciiTest(DataSet<?> dataset, boolean widePartitions, Scenario scenario)
    {
        super(dataset, widePartitions, scenario);
    }

    @Test
    public void test() throws Throwable
    {
        runIndexQueryScenarios();
    }
}
