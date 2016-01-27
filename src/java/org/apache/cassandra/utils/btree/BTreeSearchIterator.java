/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.btree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.SearchIterator;

import static org.apache.cassandra.utils.btree.BTree.lower;
import static org.apache.cassandra.utils.btree.BTree.size;

public class BTreeSearchIterator<K, V> extends TreeCursor<K> implements SearchIterator<K, V>, Iterator<V>
{
    private final boolean forwards;

    // for simplicity, we just always use the index feature of the btree to maintain our bounds within the tree,
    // whether or not they are constrained
    private int index;
    private byte state;
    public final int lowerBound, upperBound; // inclusive

    private static final int MIDDLE = 0; // only "exists" as an absence of other states
    private static final int ON_ITEM = 1; // may only co-exist with LAST (or MIDDLE, which is 0)
    private static final int BEFORE_FIRST = 2; // may not coexist with any other state
    private static final int LAST = 4; // may co-exist with ON_ITEM, in which case we are also at END
    private static final int END = 5; // equal to LAST | ON_ITEM

    public BTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir)
    {
        this(btree, comparator, dir, 0, size(btree)-1);
    }

    BTreeSearchIterator(Object[] btree, Comparator<? super K> comparator, BTree.Dir dir, int lowerBound, int upperBound)
    {
        super(comparator, btree);
        this.forwards = dir == BTree.Dir.ASC;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        rewind();
    }

    /**
     * @return 0 if we are on the last item, 1 if we are past the last item, and -1 if we are before it
     */
    private int compareToLast(int idx)
    {
        return forwards ? idx - upperBound : lowerBound - idx;
    }

    private int compareToFirst(int idx)
    {
        return forwards ? idx - lowerBound : upperBound - idx;
    }

    public boolean hasNext()
    {
        return state != END;
    }

    public V next()
    {
        switch (state)
        {
            case ON_ITEM:
                if (compareToLast(index = moveOne(forwards)) >= 0)
                    state = END;
                break;
            case BEFORE_FIRST:
                seekTo(index = forwards ? lowerBound : upperBound);
                state = (byte) (upperBound == lowerBound ? LAST : MIDDLE);
            case LAST:
            case MIDDLE:
                state |= ON_ITEM;
                break;
            default:
                throw new NoSuchElementException();
        }

        return current();
    }

    public V next(K target)
    {
        if (!hasNext())
            return null;

        int state = this.state;
        boolean found = seekTo(target, forwards, (state & (ON_ITEM | BEFORE_FIRST)) != 0);
        int index = cur.globalIndex();

        V next = null;
        if (state == BEFORE_FIRST && compareToFirst(index) < 0)
            return null;

        int compareToLast = compareToLast(index);
        if ((compareToLast <= 0))
        {
            state = compareToLast < 0 ? MIDDLE : LAST;
            if (found)
            {
                state |= ON_ITEM;
                next = (V) currentValue();
            }
        }
        else state = END;

        this.state = (byte) state;
        this.index = index;
        return next;
    }

    /**
     * Reset this Iterator to its starting position
     */
    public void rewind()
    {
        if (upperBound < lowerBound)
        {
            state = (byte) END;
        }
        else
        {
            // we don't move into the tree until the first request is made, so we know where to go
            reset(forwards);
            state = (byte) BEFORE_FIRST;
        }
        this.index = forwards ? lowerBound - 1 : upperBound + 1;
    }

    private void checkOnItem()
    {
        if ((state & ON_ITEM) != ON_ITEM)
            throw new NoSuchElementException();
    }

    public V current()
    {
        checkOnItem();
        return (V) currentValue();
    }

    public int indexOfCurrent()
    {
        checkOnItem();
        return compareToFirst(index);
    }

    private boolean onItem()
    {
        return (state & ON_ITEM) == ON_ITEM;
    }

    @DontInline
    private int nextIndex()
    {
        boolean onItem = onItem();
        int index = this.index;
        if (forwards)
        {
            index = Math.max(lowerBound, index);
            if (onItem)
                index++;
        }
        else
        {
            index = Math.min(upperBound, index);
            if (onItem)
                index--;
        }
        return index;
    }

    @DontInline
    private int moveToEnd()
    {
        boolean forwards = this.forwards;
        int index = forwards ? upperBound : lowerBound;
        this.index = next(index, forwards);
        this.state = END;
        this.reset(!forwards);
        return index;
    }

    private static int next(int index, boolean asc)
    {
        return addOne(index, !asc);
    }

    private static int prev(int index, boolean asc)
    {
        return addOne(index, asc);
    }

    private static int addOne(int index, boolean flip)
    {
        if (flip)
            return index - 1;
        else
            return index + 1;
    }

    public BTreeSearchIterator<K, V> slice(K lb, boolean lbInclusive, K ub, boolean ubInclusive)
    {
        boolean forwards = this.forwards;

        int lbIndex;
        if (lb != null)
        {
            boolean onItem = next(lb) != null;
            lbIndex = index;

            if (onItem)
            {
                // we unset ON_ITEM so that the next search includes the current key, in case we have the same ub as lb
                state &= ~ON_ITEM;
                // if we found an exact match but are exclusive, we must move the resulting index forwards one
                if (!lbInclusive)
                    lbIndex = next(lbIndex, forwards);
            }
        }
        else
        {
            lbIndex = nextIndex();
        }

        int ubIndex;
        if (ub != null)
        {
            boolean onItem = next(ub) != null;
            ubIndex = index;

            // if we found an exact match but are exclusive, we must re-include the item for the outer iterator
            if (onItem & !ubInclusive)
                state &= ~ON_ITEM;

            // if we are an exclusive match then we must move back one, as our bounds are inclusive;
            // if we are an inexact match, we are also past our bound as we find the next item greater (in iterator order)
            if (!onItem | !ubInclusive)
                ubIndex = prev(ubIndex, forwards);
        }
        else
        {
            ubIndex = moveToEnd();
        }

        // the params to our iterator are in comparator order, but we visit them in iteration order, so we must swap them
        if (!forwards)
        {
            int tIndex = lbIndex;
            lbIndex = ubIndex;
            ubIndex = tIndex;
        }

        // truncate our bounds to those defined by the outer iterator
        if (forwards)
        {
            if (ubIndex < lowerBound)
                ubIndex = lbIndex - 1;
            else if (ubIndex > upperBound)
                ubIndex = upperBound;
        }
        else
        {
            if (lbIndex > upperBound)
                lbIndex = ubIndex + 1;
            else if (lbIndex < lowerBound)
                lbIndex = lowerBound;
        }

        return new BTreeSearchIterator<>(rootNode(), comparator, BTree.Dir.asc(forwards), lbIndex, ubIndex);
    }

    public BTreeSearchIterator<K, V> clone()
    {
        return new BTreeSearchIterator<>(rootNode(), comparator, BTree.Dir.asc(forwards), lowerBound, upperBound);
    }

    public BTreeSearchIterator<K, V> reverse()
    {
        return new BTreeSearchIterator<>(rootNode(), comparator, BTree.Dir.desc(forwards), lowerBound, upperBound);
    }
}
