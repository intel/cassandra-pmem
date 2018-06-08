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
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ValueField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MDeletionTime;

public final class PersistentComplexCell extends PersistentImmutableObject
{
    private static final ObjectField<PersistentArrayList<PersistentImmutableByteArray>> CELLS = new ObjectField<>();
    private static final ValueField<PersistentDeletionTime> COMPLEX_DELETION_TIME = new ValueField<>(PersistentDeletionTime.class);
    private static final IntField COMPLEX_DATA_TYPE = new IntField();

    private static final ObjectType<PersistentComplexCell> TYPE = ObjectType.fromFields(PersistentComplexCell.class,
                                                                                        CELLS,
                                                                                        COMPLEX_DELETION_TIME,
                                                                                        COMPLEX_DATA_TYPE);

    // constructor
    public PersistentComplexCell(boolean setComplexDeletion, MDeletionTime deletionTime)
    {
        super(TYPE, (PersistentComplexCell self) ->
        {
            self.initObjectField(CELLS, new PersistentArrayList<>());
            if (setComplexDeletion)
                self.initObjectField(COMPLEX_DELETION_TIME, (PersistentDeletionTime) deletionTime);
        });
    }

    public PersistentComplexCell(int dataType, boolean setComplexDeletion, MDeletionTime deletionTime)
    {
        super(TYPE, (PersistentComplexCell self) ->
        {
            self.initObjectField(CELLS, new PersistentArrayList<>());
            self.initIntField(COMPLEX_DATA_TYPE, dataType);
            if (setComplexDeletion)
                self.initObjectField(COMPLEX_DELETION_TIME, (PersistentDeletionTime) deletionTime);
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentComplexCell(ObjectPointer<? extends PersistentComplexCell> pointer)
    {
        super(pointer);
    }

    public PersistentArrayList<PersistentImmutableByteArray> getCells()
    {
        return getObjectField(CELLS);
    }

    public void addCell(PersistentImmutableByteArray cell)
    {
        getCells().add(cell);
    }
}
