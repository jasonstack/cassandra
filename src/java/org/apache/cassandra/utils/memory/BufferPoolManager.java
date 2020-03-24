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

package org.apache.cassandra.utils.memory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class BufferPoolManager
{
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManager.class);

    /**
     * Used by chunk cache to store decompressed data and buffers may be held by chunk cache for arbitrary period.
     */
    private static final long FILE_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;
    private static final BufferPool PERMANENT = new BufferPool("Permanent", FILE_MEMORY_USAGE_THRESHOLD);

    /**
     * Used by client-server or inter-node requests, buffers should be released immediately after use.
     */
    private static final long TEMPORARY_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getTemporaryCacheSizeInMB() * 1024L * 1024L;
    private static final BufferPool TEMPORARY = new BufferPool("Temporary", TEMPORARY_MEMORY_USAGE_THRESHOLD);

    static
    {
        logger.info("Global buffer pool limit is {} for {} and {} for {}",
                    prettyPrintMemory(FILE_MEMORY_USAGE_THRESHOLD), PERMANENT.name,
                    prettyPrintMemory(TEMPORARY_MEMORY_USAGE_THRESHOLD), TEMPORARY.name);
    }
    /**
     * Long-lived buffers used for chunk cache and other disk access
     */
    public static BufferPool permanent()
    {
        return PERMANENT;
    }

    /**
     * Short-lived buffers used for internode messaging or client-server connections.
     */
    public static BufferPool temporary()
    {
        return TEMPORARY;
    }

    public static void shutdownLocalCleaner(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException
    {
        PERMANENT.shutdownLocalCleaner(timeout, unit);
        TEMPORARY.shutdownLocalCleaner(timeout, unit);
    }

}
