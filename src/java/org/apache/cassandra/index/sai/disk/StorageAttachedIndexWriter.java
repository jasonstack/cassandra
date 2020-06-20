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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Writes all on-disk index structures attached to a given SSTable.
 */
public class StorageAttachedIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL = initMemtableFlushExecutor();

    private static JMXEnabledThreadPoolExecutor initMemtableFlushExecutor()
    {
        JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, DatabaseDescriptor.getFlushWriters(), 1, TimeUnit.MINUTES,
                                                                                   new LinkedBlockingQueue<>(),
                                                                                   new NamedThreadFactory("Memtable-Index-Flush"),
                                                                                   "internal");

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        return executor;
    }

    private final Descriptor descriptor;
    private final Collection<StorageAttachedIndex> indices;
    private final Collection<ColumnIndexWriter> columnIndexWriters;
    private final SSTableComponentsWriter sstableComponentsWriter;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final RowMapping rowMapping;

    private DecoratedKey currentKey;
    private boolean tokenOffsetWriterCompleted = false;
    private boolean aborted = false;

    private long sstableRowId = 0;

    public StorageAttachedIndexWriter(Descriptor descriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker tracker) throws IOException
    {
        this(descriptor, indices, tracker, false);
    }

    public StorageAttachedIndexWriter(Descriptor descriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker tracker,
                                      boolean perColumnOnly) throws IOException
    {
        this.descriptor = descriptor;
        this.indices = indices;
        this.rowMapping = RowMapping.create(tracker.opType());
        this.columnIndexWriters = indices.stream().map(i -> i.newIndexWriter(descriptor, tracker, rowMapping))
                                         .filter(Objects::nonNull) // a null here means the column had no data to flush
                                         .collect(Collectors.toList());

        this.sstableComponentsWriter = perColumnOnly ? SSTableComponentsWriter.NONE : new SSTableComponentsWriter(descriptor);
    }

    @Override
    public void begin()
    {
        logger.debug(logMessage("Starting partition iteration for storage attached index flush for SSTable {}..."), descriptor);
        stopwatch.start();
    }

    @Override
    public void startPartition(DecoratedKey key, long position)
    {
        if (aborted) return;
        
        currentKey = key;
        sstableComponentsWriter.startPartition(key, position);
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
    {
        if (aborted) return;
        
        try
        {
            // Ignore range tombstones...
            if (unfiltered.isRow())
            {
                sstableComponentsWriter.nextUnfilteredCluster(unfiltered, position);
                rowMapping.add(currentKey, unfiltered, sstableRowId);

                for (ColumnIndexWriter w : columnIndexWriters)
                {
                    w.addRow(currentKey, sstableRowId, (Row) unfiltered);
                }

                sstableRowId++;
            }
        }
        catch (Throwable t)
        {
            abort(t, true);
        }
    }

    @Override
    public void partitionLevelDeletion(DeletionTime deletionTime, long position)
    {
        // Deletions (including partition deletions) are accounted for during reads.
    }

    @Override
    public void staticRow(Row staticRow, long position)
    {
        if (aborted) return;
        
        if (staticRow.isEmpty())
            return;

        try
        {
            sstableComponentsWriter.staticRow(staticRow, position);
            rowMapping.add(currentKey, staticRow, sstableRowId);

            for (ColumnIndexWriter w : columnIndexWriters)
            {
                w.addRow(currentKey, sstableRowId, staticRow);
            }

            sstableRowId++;
        }
        catch (Throwable t)
        {
            abort(t, true);
        }
    }

    @Override
    public void complete()
    {
        if (aborted) return;
        
        logger.debug(logMessage("Completed partition iteration for index flush for SSTable {}. Elapsed time: {} ms"),
                     descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        try
        {
            sstableComponentsWriter.complete();
            tokenOffsetWriterCompleted = true;

            logger.debug(logMessage("Flushed tokens and offsets for SSTable {}. Elapsed time: {} ms."),
                         descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            rowMapping.complete();

            if (rowMapping == RowMapping.DUMMY)
            {
                for (ColumnIndexWriter columnIndexWriter : columnIndexWriters)
                {
                    columnIndexWriter.flush();
                }
            }
            else
            {
                List<Future> futures = new ArrayList<>(columnIndexWriters.size());
                for (ColumnIndexWriter columnIndexWriter : columnIndexWriters)
                {
                    futures.add(INDEX_FLUSHER_GENERAL.submit(() -> {
                        columnIndexWriter.flush();
                        return null;
                    }));
                }
                FBUtilities.allOf(futures).get();
            }
        }
        catch (Throwable e)
        {
            abort(e, true);
        }
    }

    /**
     * Aborts all column index writers and, only if they have not yet completed, SSTable-level component writers.
     * 
     * @param accumulator the initial exception thrown from the failed writer
     */
    @Override
    public void abort(Throwable accumulator)
    {
        abort(accumulator, false);
    }

    /**
     *
     * @param accumulator original cause of the abort
     * @param fromIndex true if the cause of the abort was the index itself, false otherwise
     */
    public void abort(Throwable accumulator, boolean fromIndex)
    {
        // Mark the write aborted, so we can short-circuit any further operations on the component writers.
        aborted = true;
        
        // Make any indexes involved in this transaction non-queryable, as they will likely not match the backing table.
        if (fromIndex)
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
        
        for (ColumnIndexWriter writer : columnIndexWriters)
        {
            try
            {
                writer.abort(accumulator);
            }
            catch (Throwable t)
            {
                if (accumulator != null)
                {
                    accumulator.addSuppressed(t);
                }
            }
        }
        
        if (!tokenOffsetWriterCompleted)
        {
            // If the token/offset files have already been written successfully, they can be reused later. 
            sstableComponentsWriter.abort(accumulator);
        }
    }

    /**
     * A helper method for constructing consistent log messages. This method is different to similar helper
     * methods in that log messages generated in this class are not necessarily related to a single index
     * so the log message is decorated as follows:
     *
     * [ks.tb.*] Log message
     *
     * @param message The raw content of a logging message.
     *
     * @return A log message with the proper keyspace and table name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s", descriptor.ksname, descriptor.cfname, message);
    }

}
