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
package org.apache.cassandra.index.sai.disk;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.RangeIterator;

import static org.apache.cassandra.index.sai.disk.KDTreeIndexBuilder.buildInt32Searcher;
import static org.apache.cassandra.index.sai.disk.KDTreeIndexBuilder.buildLongSearcher;
import static org.apache.cassandra.index.sai.disk.KDTreeIndexBuilder.buildShortSearcher;

public class KDTreeIndexSearcherTest extends NdiRandomizedTest
{
    @Test
    public void testRangeQueriesAgainstInt32Index() throws Exception
    {
        final IndexSearcher indexSearcher = buildInt32Searcher(newIndexComponents(), 0, 10);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(2), false);
            upper = new Bound(Int32Type.instance.decompose(7), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(3, results.next().getLong());
            assertEquals(4, results.next().getLong());
            assertEquals(5, results.next().getLong());
            assertEquals(6, results.next().getLong());
            assertEquals(7, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(10), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", Int32Type.instance)
        {{
            operation = Op.RANGE;
            upper = new Bound(Int32Type.instance.decompose(0), false);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testEqQueriesAgainstInt32Index() throws Exception
    {
        final IndexSearcher indexSearcher = buildInt32Searcher(newIndexComponents(), 0, 3);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", Int32Type.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(Int32Type.instance.decompose(0), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(0, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", Int32Type.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(Int32Type.instance.decompose(3), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testRangeQueriesAgainstLongIndex() throws Exception
    {
        final IndexSearcher indexSearcher = buildLongSearcher(newIndexComponents(), 0, 10);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", LongType.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(LongType.instance.decompose(2L), false);
            upper = new Bound(LongType.instance.decompose(7L), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(3L, results.next().getLong());
            assertEquals(4L, results.next().getLong());
            assertEquals(5L, results.next().getLong());
            assertEquals(6L, results.next().getLong());
            assertEquals(7L, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", LongType.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(LongType.instance.decompose(10L), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", LongType.instance)
        {{
            operation = Op.RANGE;
            upper = new Bound(LongType.instance.decompose(0L), false);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testEqQueriesAgainstLongIndex() throws Exception
    {
        final IndexSearcher indexSearcher = buildLongSearcher(newIndexComponents(), 0, 3);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", LongType.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(LongType.instance.decompose(0L), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(0L, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", LongType.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(LongType.instance.decompose(3L), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testRangeQueriesAgainstShortIndex() throws Exception
    {
        final IndexSearcher indexSearcher = buildShortSearcher(newIndexComponents(), (short) 0, (short) 10);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", ShortType.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(ShortType.instance.decompose((short) 2), false);
            upper = new Bound(ShortType.instance.decompose((short) 7), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(3L, results.next().getLong());
            assertEquals(4L, results.next().getLong());
            assertEquals(5L, results.next().getLong());
            assertEquals(6L, results.next().getLong());
            assertEquals(7L, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", ShortType.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(ShortType.instance.decompose((short) 10), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", ShortType.instance)
        {{
            operation = Op.RANGE;
            upper = new Bound(ShortType.instance.decompose((short) 0), false);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testEqQueriesAgainstShortIndex() throws Exception
    {
        final IndexSearcher indexSearcher = buildShortSearcher(newIndexComponents(), (short) 0, (short) 3);
        try (RangeIterator results = indexSearcher.search(new Expression("meh", ShortType.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(ShortType.instance.decompose((short) 0), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertEquals(0L, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression("meh", ShortType.instance)
        {{
            operation = Op.EQ;
            lower = upper = new Bound(ShortType.instance.decompose((short) 3), true);
        }}, SSTableQueryContext.forTest()))
        {
            assertFalse(results.hasNext());
        }

        indexSearcher.close();
    }

    @Test
    public void testUnsupportedOperator() throws Exception
    {
        final IndexSearcher indexSearcher = buildShortSearcher(newIndexComponents(), (short) 0, (short) 3);
        try
        {
            indexSearcher.search(new Expression("meh", ShortType.instance)
            {{
                operation = Op.NOT_EQ;
                lower = upper = new Bound(ShortType.instance.decompose((short) 0), true);
            }}, SSTableQueryContext.forTest());

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }
}
