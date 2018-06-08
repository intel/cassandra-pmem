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
import lib.util.persistent.PersistentString;
import lib.util.persistent.types.BooleanField;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.ObjectType;
import lib.util.persistent.types.StringField;
import org.apache.cassandra.memory.MColumnMetadata;

public final class PersistentColumnMetadata extends PersistentImmutableObject implements MColumnMetadata
{
    private static final StringField COLUMN_NAME = new StringField();
    private static final IntField DATA_TYPE = new IntField();
    private static final BooleanField IS_MULTI_CELL = new BooleanField();

    private static final ObjectType<PersistentColumnMetadata> TYPE = ObjectType.fromFields(PersistentColumnMetadata.class,
                                                                                           COLUMN_NAME,
                                                                                           DATA_TYPE,
                                                                                          /* COLUMN_TYPE,*/
                                                                                           IS_MULTI_CELL);

    // constructor
    public PersistentColumnMetadata(PersistentString columnName, int dataType, boolean flag)
    {
        super(TYPE, (PersistentColumnMetadata self) ->
        {
            self.initObjectField(COLUMN_NAME, columnName);
            self.initIntField(DATA_TYPE, dataType);
            self.initBooleanField(IS_MULTI_CELL, flag);
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentColumnMetadata(ObjectPointer<? extends PersistentColumnMetadata> pointer)
    {
        super(pointer);
    }

    @Override
    public String getColumnName()
    {
        return getObjectField(COLUMN_NAME).toString();
    }

    public PersistentString getPColumnName()
    {
        return getObjectField(COLUMN_NAME);
    }

    public int getPMDataType()
    {
        return getIntField(DATA_TYPE);
    }

    @Override
    public boolean getIsMultiCell()
    {
        return getBooleanField(IS_MULTI_CELL);
    }
}
