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
package org.apache.cassandra.db.rows;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.ColumnInfo.VirtualCells;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

public class VirtualCellsTest
{

    @Test
    public void testMergeColumnInfo()
    {
        // both live, greater ts wins
        ColumnInfo ci1 = new ColumnInfo(100L, 0, Integer.MAX_VALUE); // live
        ColumnInfo ci2 = new ColumnInfo(101L, 0, Integer.MAX_VALUE); // live
        ColumnInfo merged = ci1.merge(ci2);
        assertEquals(101L, merged.timestamp());
        assertEquals(0, merged.ttl());
        assertEquals(Integer.MAX_VALUE, merged.localDeletionTime());

        // live superseds tombstone
        ci1 = new ColumnInfo(100L, 0, Integer.MAX_VALUE); // live
        ci2 = new ColumnInfo(99L, 0, Integer.MAX_VALUE - 1); // dead
        merged = ci1.merge(ci2);
        assertEquals(100L, merged.timestamp());
        assertEquals(0, merged.ttl());
        assertEquals(Integer.MAX_VALUE, merged.localDeletionTime());

        // tombstone superseds live
        ci1 = new ColumnInfo(100L, 0, Integer.MAX_VALUE); // live
        ci2 = new ColumnInfo(100L, 0, Integer.MAX_VALUE - 1); // dead
        merged = ci1.merge(ci2);
        assertEquals(100L, merged.timestamp());
        assertEquals(0, merged.ttl());
        assertEquals(Integer.MAX_VALUE - 1, merged.localDeletionTime());

        // ttl, larger localDeletionTime win
        ci1 = new ColumnInfo(100L, 5, 100);
        ci2 = new ColumnInfo(100L, 3, 200);
        merged = ci1.merge(ci2);
        assertEquals(100L, merged.timestamp());
        assertEquals(3, merged.ttl());
        assertEquals(200, merged.localDeletionTime());

        // ttl vs liv, larger timestamp win
        ci1 = new ColumnInfo(101L, 0, Integer.MAX_VALUE);
        ci2 = new ColumnInfo(100L, 3, 200);
        merged = ci1.merge(ci2);
        assertEquals(101L, merged.timestamp());
        assertEquals(0, merged.ttl());
        assertEquals(Integer.MAX_VALUE, merged.localDeletionTime());
    }

    @Test
    public void testMergeVirtualCells()
    {
        // live keyOrConditions, tombstone unselected
        VirtualCells vc1 = VirtualCells.create(liveVirtualCells("a", 1L, "b", 2L), liveVirtualCells("c", 1L));
        VirtualCells vc2 = VirtualCells.create(liveVirtualCells("a", 2L, "b", 1L),
                                               tombstoneVirtualCellc("c", 2L, 1000));
        VirtualCells merged = vc1.merge(vc2);
        assertFalse(merged.isEmpty());
        assertFalse(merged.shouldWipeRow(FBUtilities.nowInSeconds()));
        assertEquals(liveVirtualCells("a", 2L, "b", 2L), merged.getKeyOrConditions());
        assertEquals(tombstoneVirtualCellc("c", 2L, 1000), merged.getUnselected());

        // tombstone keyOrConditions, tombstone unselected
        vc1 = VirtualCells.create(tombstoneVirtualCellc("a", 1L, 100, "b", 2L, 100), liveVirtualCells("c", 1L));
        vc2 = VirtualCells.create(liveVirtualCells("a", 1L, "b", 2L),
                                  tombstoneVirtualCellc("c", 2L, 1000));
        merged = vc1.merge(vc2);
        assertFalse(merged.isEmpty());
        assertTrue(merged.shouldWipeRow(FBUtilities.nowInSeconds()));
        assertEquals(tombstoneVirtualCellc("a", 1L, 100, "b", 2L, 100), merged.getKeyOrConditions());
        assertEquals(tombstoneVirtualCellc("c", 2L, 1000), merged.getUnselected());
    }

    /**
     * ColumnName(String)-Timestamp(Long)
     */
    private Map<String, ColumnInfo> liveVirtualCells(Object... objs)
    {
        if (objs == null || objs.length == 0)
            return Collections.EMPTY_MAP;
        assert objs.length % 2 == 0;

        Map<String, ColumnInfo> info = new HashMap<>();
        for (int i = 0; i < objs.length; i += 2)
        {
            info.put((String) objs[i], new ColumnInfo((long) objs[i + 1], 0, Integer.MAX_VALUE));
        }
        return info;
    }

    /**
     * ColumnName(String)-Timestamp(Long)-LocalDeletionTime(Integer)
     */
    private Map<String, ColumnInfo> tombstoneVirtualCellc(Object... objs)
    {
        if (objs == null || objs.length == 0)
            return Collections.EMPTY_MAP;
        assert objs.length % 3 == 0;

        Map<String, ColumnInfo> info = new HashMap<>();
        for (int i = 0; i < objs.length; i += 3)
        {
            info.put((String) objs[i], new ColumnInfo((long) objs[i + 1], 0, (int) objs[i + 2]));
        }
        return info;
    }
}
