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
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.utils.FastByteOperations;

public final class PersistentDecoratedKey extends PersistentImmutableObject implements Comparable<PersistentDecoratedKey>,
                                                                                       MDecoratedKey,
                                                                                       ComparableWith<DecoratedKey>
{
    private static final ObjectField<PersistentImmutableByteArray> PARTITION_KEY_BUFFER = new ObjectField<>();
    private static final ObjectField<PMToken> TOKEN = new ObjectField<>();

    private static final ObjectType<PersistentDecoratedKey> TYPE = ObjectType.fromFields(PersistentDecoratedKey.class,
                                                                                         PARTITION_KEY_BUFFER, TOKEN);

    // constructor
    public PersistentDecoratedKey(byte[] partitionKeyBuffer, MToken token)
    {
        super(TYPE, (PersistentDecoratedKey self) ->
        {
            self.initObjectField(PARTITION_KEY_BUFFER, new PersistentImmutableByteArray(partitionKeyBuffer));
            self.initObjectField(TOKEN, (PMToken) token);
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentDecoratedKey(ObjectPointer<? extends PersistentDecoratedKey> pointer)
    {
        super(pointer);
    }

    @Override
    public byte[] getKey()
    {
        return getObjectField(PARTITION_KEY_BUFFER).toArray();
    }

    @Override
    public MToken getToken()
    {
        return getObjectField(TOKEN);
    }

    @Override
    public int compareTo(PersistentDecoratedKey pos)
    {
        if (this == pos)
            return 0;

        byte[] b1 = getKey();
        byte[] b2 = pos.getKey();
        int cmp = getToken().compareTo(pos.getToken());
        return cmp == 0 ? FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length) : cmp;
    }

    @Override
    public int compareWith(DecoratedKey pos)
    {
        byte[] b1 = getKey();
        byte[] b2 = pos.getKey().array();
        int cmp = ((PMToken) getToken()).compareWith(pos.getToken());
        cmp = cmp == 0 ?
              FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length) : cmp;
        return cmp;
    }

    @Override
    public int hashCode()
    {
        byte[] key = getKey();
        int h = 1;
        int p = 0;
        for (int i = key.length - 1; i >= p; i--)
            h = 31 * h + (int) key[i];
        return h;
    }

    @Override
    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;

        if (obj instanceof PersistentDecoratedKey)
        {
            if (this == obj)
                return true;
            return compareTo((PersistentDecoratedKey) obj) == 0 ? true : false;
        }

        if (obj instanceof DecoratedKey)
            return compareWith((DecoratedKey) obj) == 0 ? true : false;

        return false;
    }
}

