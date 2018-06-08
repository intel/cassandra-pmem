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
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.PersistentSIHashMap;
import lib.util.persistent.PersistentUUID;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTablesManager;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This class manages the mapping info between each table unique identifier and its corresponding
 * PMTable instance.
 */
public final class PMTablesManager extends PersistentImmutableObject implements MTablesManager
{

    private static final FinalObjectField<PersistentSIHashMap<PersistentUUID, PMTable>> PMTABLE_MANAGER = new FinalObjectField<>();
    private static final ObjectType<PMTablesManager> TYPE = ObjectType.fromFields(PMTablesManager.class, PMTABLE_MANAGER);

    // constructor
    public PMTablesManager()
    {
        super(TYPE, (PMTablesManager self) ->
        {
            self.initObjectField(PMTABLE_MANAGER, new PersistentSIHashMap<>());
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PMTablesManager(ObjectPointer<? extends PMTablesManager> pointer)
    {
        super(pointer);
    }

    private PersistentSIHashMap<PersistentUUID, PMTable> getPMTablesManager()
    {
        return getObjectField(PMTABLE_MANAGER);
    }

    // Check if a PMTable instance exists for a given table id
    @Override
    public boolean doesMTableExist(UUID tableId)
    {
        return getPMTablesManager().containsKey(new PersistentUUID(tableId.getMostSignificantBits(), tableId.getLeastSignificantBits()));
    }

    // Create a PMTable for a given table id if one doesn't exist already. Ideally, there should be
    // not attempt to create a table again. Once a table is created for a given id, it can only be
    // deleted and then inserted again.
    @Override
    public boolean createMTableIfAbsent(UUID tableId, boolean isClusteringKeyAvailable,
                                        MTableMetadata tableMetadata, TableMetadata tempMetadata)
    {
        PMTable pmTable = getPMTablesManager().putIfAbsent(new PersistentUUID(tableId.getMostSignificantBits(),
                                                                              tableId.getLeastSignificantBits()),
                                                           (PMTable) MTable.getInstance(tableMetadata,
                                                                                        isClusteringKeyAvailable,
                                                                                        tempMetadata));
        if (pmTable == null)
            return true;
        else
            return false;
    }

    // Return PMTable instance for a given table id
    @Override
    public PMTable getMTable(UUID tableId)
    {
        return getPMTablesManager().get(tableId, PersistentUUID.class);
    }
}
