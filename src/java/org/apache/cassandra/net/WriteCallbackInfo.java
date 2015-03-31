/*
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
 */
package org.apache.cassandra.net;

import java.net.InetAddress;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.StorageProxy;

public class WriteCallbackInfo extends CallbackInfo
{
    public final MessageOut sentMessage;
    private final ConsistencyLevel consistencyLevel;
    private final boolean allowHints;

    public WriteCallbackInfo(InetAddress target,
                             IAsyncCallback callback,
                             MessageOut message,
                             IVersionedSerializer<?> serializer,
                             ConsistencyLevel consistencyLevel,
                             boolean allowHints)
    {
        super(target, callback, serializer, true);
        assert message != null;
        this.sentMessage = message;
        this.consistencyLevel = consistencyLevel;
        this.allowHints = allowHints;
    }

    public boolean shouldHint()
    {
        return allowHints
            && sentMessage.verb != MessagingService.Verb.COUNTER_MUTATION
            && consistencyLevel != ConsistencyLevel.ANY
            && StorageProxy.shouldHint(target);
    }
}
