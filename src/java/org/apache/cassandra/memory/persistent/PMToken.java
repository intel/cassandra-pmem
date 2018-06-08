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

package org.apache.cassandra.memory.persistent;

import lib.util.persistent.ComparableWith;
import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.memory.MToken;

public abstract class PMToken extends PersistentImmutableObject implements MToken, ComparableWith<Token>
{
    public static final ObjectType<PMToken> TYPE = ObjectType.fromFields(PMToken.class);

    // subclassing constructor
    protected<T extends PMToken> PMToken(ObjectType<T> type, java.util.function.Consumer<T> initializer)
    {
        super(type, (PMToken self) ->
        {
            if (initializer != null) initializer.accept((T) self);
        });
    }

    // reconstructor
    protected PMToken(ObjectPointer<? extends PMToken> pointer)
    {
        super(pointer);
    }

    public static <T> PMToken getInstance(T token)
    {
        if (DatabaseDescriptor.getPartitionerName().equalsIgnoreCase("org.apache.cassandra.dht.Murmur3Partitioner"))
            return new PersistentLongToken((Long) token);
        else if (DatabaseDescriptor.getPartitionerName().equalsIgnoreCase("org.apache.cassandra.dht.ByteOrderedPartitioner"))
            return new PersistentBytesToken((byte[]) token);
        return null;
    }

    public abstract <T> T getToken();

    public abstract <T> int compareTo(T token);

    public abstract int compareWith(Token token);
}
