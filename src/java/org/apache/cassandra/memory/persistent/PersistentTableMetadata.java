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

import java.util.UUID;

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.PersistentSIHashMap;
import lib.util.persistent.PersistentString;
import lib.util.persistent.PersistentUUID;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import lib.util.persistent.types.StringField;
import org.apache.cassandra.memory.MColumnMetadata;
import org.apache.cassandra.memory.MTableMetadata;

public final class PersistentTableMetadata extends PersistentImmutableObject implements MTableMetadata
{
    private static final StringField KEYSPACE = new StringField();
    private static final StringField TABLE_NAME = new StringField();
    private static final ObjectField<PersistentUUID> TABLE_ID = new ObjectField<>();
    private static final ObjectField<PersistentArrayList<PersistentColumnMetadata>> PARTITION_KEY = new ObjectField<>();
    private static final ObjectField<PersistentArrayList<PersistentColumnMetadata>> CLUSTERING_KEY = new ObjectField<>();
    private static final ObjectField<PersistentSIHashMap<PersistentString, PersistentColumnMetadata>> REGULAR_STATIC_COLUMNS = new ObjectField<>();
    private static final ObjectType<PersistentTableMetadata> TYPE = ObjectType.fromFields(PersistentTableMetadata.class,
                                                                                          KEYSPACE,
                                                                                          TABLE_NAME,
                                                                                          TABLE_ID,
                                                                                          PARTITION_KEY,
                                                                                          CLUSTERING_KEY,
                                                                                          REGULAR_STATIC_COLUMNS);

    // constructor
    public PersistentTableMetadata(String keyspace, String tableName, UUID tableId)
    {
        super(TYPE, (PersistentTableMetadata self) ->
        {
            self.initObjectField(KEYSPACE, PersistentString.make(keyspace));
            self.initObjectField(TABLE_NAME, PersistentString.make(tableName));
            self.initObjectField(TABLE_ID, new PersistentUUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits()));
            self.initObjectField(PARTITION_KEY, new PersistentArrayList<>());
            self.initObjectField(CLUSTERING_KEY, new PersistentArrayList<>());
            self.initObjectField(REGULAR_STATIC_COLUMNS, new PersistentSIHashMap<>());
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentTableMetadata(ObjectPointer<? extends PersistentTableMetadata> pointer)
    {
        super(pointer);
    }

    @Override
    public String getKeyspace()
    {
        return getObjectField(KEYSPACE).toString();
    }

    @Override
    public String getTableName()
    {
        return getObjectField(TABLE_NAME).toString();
    }

    @Override
    public UUID getTableId()
    {
        return new UUID(getObjectField(TABLE_ID).getMostSignificantBits(), getObjectField(TABLE_ID).getLeastSignificantBits());
    }

    @Override
    public PersistentArrayList<PersistentColumnMetadata> getPartitionKey()
    {
        return getObjectField(PARTITION_KEY);
    }

    @Override
    public void addPartitionKey(MColumnMetadata columnMetadata)
    {
        getObjectField(PARTITION_KEY).add((PersistentColumnMetadata) columnMetadata);
    }

    // @Override
    public PersistentArrayList<PersistentColumnMetadata> getClusteringKey()
    {
        return getObjectField(CLUSTERING_KEY);
    }

    // @Override
    public void addClusteringKey(MColumnMetadata columnMetadata)
    {
        getClusteringKey().add((PersistentColumnMetadata) columnMetadata);
    }

    public PersistentSIHashMap<PersistentString, PersistentColumnMetadata> getRegularStaticColumns()
    {
        return getObjectField(REGULAR_STATIC_COLUMNS);
    }

    public void addRegularStaticColumn(PersistentString name, MColumnMetadata columnMetadata)
    {
        getRegularStaticColumns().put(name, (PersistentColumnMetadata) columnMetadata);
    }
}
