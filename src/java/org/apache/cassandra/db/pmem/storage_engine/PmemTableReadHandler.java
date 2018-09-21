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

package org.apache.cassandra.db.pmem.storage_engine;

import java.util.concurrent.Future;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.TableReadHandler;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.pmem.TablesManager;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public class PmemTableReadHandler implements TableReadHandler
{

    public UnfilteredPartitionIterator readPartitionRange(ColumnFamilyStore cfs, PartitionRangeReadCommand partitionRangeReadCommand) throws Exception
    {
        TablesManager tablesManager = PmemStorageHandler.getInstance().getTablesManager(cfs.keyspace.getName());
        return tablesManager.makePartitionIterator(cfs, partitionRangeReadCommand);
    }

    public Future<UnfilteredRowIterator> readSinglePartition(ColumnFamilyStore cfs, ColumnFilter colFilter, DecoratedKey partitionKey)
    {
        TablesManager tablesManager = PmemStorageHandler.getInstance().getTablesManager(cfs.keyspace.getName());
        return tablesManager.makeRowIterator(cfs, colFilter,partitionKey);
    }

    public Future<UnfilteredRowIterator> readSinglePartitionWithFilter(ColumnFamilyStore cfs, ColumnFilter colFilter,ClusteringIndexFilter filter, DecoratedKey partitionKey)
    {
        TablesManager tablesManager = PmemStorageHandler.getInstance().getTablesManager(cfs.keyspace.getName());
        return tablesManager.makeRowIterator(cfs, colFilter, filter, partitionKey);
    }
}
