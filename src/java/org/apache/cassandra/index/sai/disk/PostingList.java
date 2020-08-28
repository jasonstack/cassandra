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

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Interface for advancing on and consuming a posting list.
 */
@NotThreadSafe
public interface PostingList extends Closeable
{
    int OFFSET_NOT_FOUND = -1;
    int END_OF_STREAM = DocIdSetIterator.NO_MORE_DOCS;

    @Override
    default void close() throws IOException {}

    /**
     * Retrieves the next segment row ID, not including row IDs that have been returned by {@link #advance(int)}.
     *
     * @return next segment row ID
     */
    int nextPosting() throws IOException;

    int size();

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     *
     * Note: Callers must use the return value of this method before calling {@link #nextPosting()}, as calling
     * that method will return the next posting, not the one to which we have just advanced.
     *
     * @param targetRowID target row ID to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */
    int advance(int targetRowID) throws IOException;

    /**
     * @return peekable wrapper of current posting list
     */
    default PeekablePostingList peekable()
    {
        return new PeekablePostingList(this);
    }

    public static class PeekablePostingList implements PostingList
    {
        private final PostingList wrapped;

        private boolean peeked = false;
        private int next;

        public PeekablePostingList(PostingList wrapped)
        {
            this.wrapped = wrapped;
        }

        public int peek()
        {
            if (peeked)
                return next;

            try
            {
                peeked = true;
                return next = wrapped.nextPosting();
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        @Override
        public int nextPosting() throws IOException
        {
            if (peeked)
            {
                peeked = false;
                return next;
            }
            return wrapped.nextPosting();
        }

        @Override
        public int size()
        {
            return wrapped.size() + (peeked ? 1 : 0);
        }

        @Override
        public int advance(int targetRowID) throws IOException
        {
            if (peeked && next >= targetRowID)
            {
                peeked = false;
                return next;
            }

            peeked = false;
            return wrapped.advance(targetRowID);
        }

        @Override
        public void close() throws IOException
        {
            wrapped.close();
        }
    }
}
