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

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.LongField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MDeletionTime;

public final class PersistentDeletionTime extends PersistentImmutableObject implements Comparable<PersistentDeletionTime>, MDeletionTime
{
    private static final LongField MARKED_FOR_DELETE_AT = new LongField();
    private static final IntField LOCAL_DELETION_TIME = new IntField();

    private static final ObjectType<PersistentDeletionTime> TYPE = ObjectType.withValueFields(PersistentDeletionTime.class,
                                                                                              MARKED_FOR_DELETE_AT, LOCAL_DELETION_TIME);

    // constructor
    public PersistentDeletionTime(long markedForDeleteAt, int localDeletionTime)
    {
        super(TYPE, (PersistentDeletionTime self) ->
        {
            self.initLongField(MARKED_FOR_DELETE_AT, markedForDeleteAt);
            self.initIntField(LOCAL_DELETION_TIME, localDeletionTime);
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentDeletionTime(ObjectPointer<? extends PersistentDeletionTime> pointer)
    {
        super(pointer);
    }

    // setters/getters
    @Override
    public long markedForDeleteAt()
    {
        return getLongField(MARKED_FOR_DELETE_AT);
    }

    @Override
    public int localDeletionTime()
    {
        return getIntField(LOCAL_DELETION_TIME);
    }

    // other helper/inherited methods
    public boolean isLive()
    {
        return getLongField(MARKED_FOR_DELETE_AT) == Long.MIN_VALUE && getIntField(LOCAL_DELETION_TIME) == Integer
                                                                                                           .MAX_VALUE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PersistentDeletionTime))
            return false;
        PersistentDeletionTime that = (PersistentDeletionTime) o;
        return getLongField(MARKED_FOR_DELETE_AT) == that.markedForDeleteAt() && getIntField(LOCAL_DELETION_TIME) ==
                                                                                 that.localDeletionTime();
    }

    @Override
    public final int hashCode()
    {
        // TODO: Uncomment hashCode method once ported to Cassandra source
        // The below hashCode() method is implemented in guava library
        // so won't compile in this project since we don't include that
        return (int) (markedForDeleteAt() + localDeletionTime());
        // return Objects.hashCode(markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public int compareTo(PersistentDeletionTime dt)
    {
        if (markedForDeleteAt() < dt.markedForDeleteAt())
            return -1;
        else if (markedForDeleteAt() > dt.markedForDeleteAt())
            return 1;
        else if (localDeletionTime() < dt.localDeletionTime())
            return -1;
        else if (localDeletionTime() > dt.localDeletionTime())
            return 1;
        else
            return 0;
    }
}

