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

import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.TableWriteHandler;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.pmem.TablesManager;
import org.apache.cassandra.db.pmem.storage_engine.PmemWriteContext;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PmemTableWriteHandler implements TableWriteHandler
{
    private final ColumnFamilyStore cfs;
    TablesManager tablesManager = null;
    private static final Logger logger = LoggerFactory.getLogger(PmemTableWriteHandler.class);

    public PmemTableWriteHandler(ColumnFamilyStore cfs)//TODO: Handle exception case where pmem not avaialble
    {
        this.cfs = cfs;
        tablesManager = PmemStorageHandler.getInstance().getTablesManager(cfs.keyspace.getName());
        if (tablesManager == null)
        {
            tablesManager = new TablesManager();
            PmemStorageHandler.getInstance().setTablesManager(cfs.keyspace.getName(),tablesManager);
        }
        tablesManager.add(cfs.metadata.id,cfs);
    }

    @Override
    public void write(PartitionUpdate update, WriteContext context, UpdateTransaction updateTransaction)
    {
        PmemWriteContext ctx = PmemWriteContext.fromContext(context);
        Tracing.trace("Writing to {} pmem", update.metadata().name);
        try
        {
            tablesManager.apply(cfs, update, updateTransaction, ctx.getGroup()).get();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
    }

    public void gc() {
        System.out.println("garbace collecting pmem");
        logger.debug("garbace collecting pmem");
        Tracing.trace("garbace collecting pmem");
        tablesManager.garbageCollect(cfs);
    }

    public void alterTable(TableMetadata metadata)
    {
        tablesManager.updateMetaData(metadata);
    }
}
