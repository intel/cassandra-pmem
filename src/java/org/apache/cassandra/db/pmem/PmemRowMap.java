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

package org.apache.cassandra.db.pmem;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lib.llpl.TransactionalHeap;
import lib.llpl.TransactionalMemoryBlock;
//import lib.llpl.Raw;
import lib.llpl.Transaction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

// keys are Clusterings, and are stored in a MemoryRegion seperate from values
// values are Rows, with the columns serialized (with assocaited header and column information) stored contiguously
// in one MemoryRegion
// -- EVENTUALLY, that's how it wil be stored. I've naively just invoked Cell.serializer.serialize for the cells ...


// basically, this is a sorted hash map, persistent and unsafe, specific to a given partition
public class PmemRowMap
{
    //private  TransactionalMemoryBlock block;
    private static TransactionalHeap heap;
    private final ARTree arTree;
    private TableMetadata tableMetadata;
    private DeletionTime partitionLevelDeletion;
    private final StatsCollector statsCollector = new StatsCollector();
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMap.class);

    public PmemRowMap(TransactionalHeap heap, ARTree arTree, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion)
    {
        this.heap = heap;
        this.arTree = arTree;
        this.tableMetadata = tableMetadata;
        this.partitionLevelDeletion = partitionLevelDeletion;
    }

    public static PmemRowMap create(TransactionalHeap heap, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion, Transaction tx)
    {
        PmemRowMap[] map = new PmemRowMap[1];
        tx.run(() -> { //TODO: this transaction not needed. revisit
     //       int headerSize = 8;
            ARTree arTree = new ARTree(heap);
            map[0] = new PmemRowMap(heap,  arTree, tableMetadata, partitionLevelDeletion);
        });
        return map[0];
    }

    public static PmemRowMap loadFromAddress(TransactionalHeap heap, long address, TableMetadata tableMetadata, DeletionTime partitionLevelDeletion)
    {
        PmemRowMap[] map = new PmemRowMap[1];
        ARTree arTree = new ARTree(heap, address);
        map[0] = new PmemRowMap(heap, arTree, tableMetadata, partitionLevelDeletion);
        return map[0];
    }

    public long getAddress()
    {
        return arTree.address();
    }

    public   Row getMergedRow(Row newRow, Long mb)
    {
        Clustering clustering = newRow.clustering();
        Row.Builder builder = BTreeRow.sortedBuilder();
        TransactionalMemoryBlock oldBlock = heap.memoryBlockFromHandle(mb);
        MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(oldBlock, heap); //TODO: Pass the heap as parameter for now until memoryblock.copyfromArray is sorted out
        SerializationHelper helper = new SerializationHelper(tableMetadata, -1, SerializationHelper.Flag.LOCAL);
        Row currentRow = null;
        //try
        //{
            builder.newRow(clustering);

            SerializationHeader serializationHeader;
            int flags = oldBlock.getByte(0);
	    //TODO: Analyze if tablesMetaDataMap.get can return null 
	    List<SerializationHeader> headerList = TablesManager.tablesMetaDataMap.get(tableMetadata.id);
            if(headerList.size() > 1)
            {
                if (PmemRowSerializer.isTableAltered(flags))
                {
                    int version = oldBlock.getByte(2);
                    serializationHeader = headerList.get(version-1);
                }
                else
                {
                    serializationHeader = headerList.get(0);
                }
            }
            else
            {
                   serializationHeader = new SerializationHeader(false,
                                                               tableMetadata,
                                                               tableMetadata.regularAndStaticColumns(),
                                                              EncodingStats.NO_STATS);
            }

        try
        {
            currentRow = (Row) PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        return(Rows.merge(currentRow, newRow));
    }

    Long merge(Object newRow, Long mb)
    {
        Row row = (Row)newRow;
        TransactionalMemoryBlock oldBlock = null;
        SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                          tableMetadata,
                                                                          tableMetadata.regularAndStaticColumns(),
                                                                          EncodingStats.NO_STATS);
                                                                         // statsCollector.get());
        if(mb !=0) //Get the existing row to merge with update
        {
            oldBlock = heap.memoryBlockFromHandle(mb);
            row = getMergedRow((Row)newRow, mb);
        }
        int version = TablesManager.tablesMetaDataMap.get(tableMetadata.id).size();
        DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get();
        try
        {
            PmemRowSerializer.serializer.serialize(row, serializationHeader, dob, 0, version );

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        int size = dob.getLength();
        TransactionalMemoryBlock cellMemoryRegion;
        if(mb == 0)
            cellMemoryRegion = heap.allocateMemoryBlock(size);
        else if(oldBlock.size() < size)
        {
            cellMemoryRegion = heap.allocateMemoryBlock(size);
            oldBlock.free();
        }
        else
        {
            cellMemoryRegion = oldBlock;//TODO: should we zero out if size is greater ?
        }
        MemoryBlockDataOutputPlus cellsOutputPlus = new MemoryBlockDataOutputPlus(cellMemoryRegion, 0);
        try
        {
            cellsOutputPlus.write(dob.getData(),0,dob.getLength());
            dob.clear();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        long rowAddress = cellMemoryRegion.handle();
        return rowAddress;
    };

    public void put(Row row, PartitionUpdate update, Transaction tx) //throws IOException
    {
        /// KEY
        Clustering clustering = row.clustering();
        ByteBuffer clusteringBuffer;
        statsCollector.update(update.stats());
        // need to get version number and list of absolute types here ...
        clusteringBuffer = Clustering.serializer.serialize(clustering,-1, update.metadata().comparator.subtypes());
        try
        {
            arTree.apply(clusteringBuffer.array(), row, this::merge, tx);
        }
        catch(IndexOutOfBoundsException e) //TODO: Refine exception handling
        {
            logger.error(e.getMessage(), e);
            e.printStackTrace();

        }
    }

    static void clearData(Long mb)
    {
        TransactionalMemoryBlock memoryBlock = heap.memoryBlockFromHandle(mb);
        memoryBlock.free();
    }

    public void delete()
    {
        arTree.clear(PmemRowMap::clearData);
    }

    private static class StatsCollector
    {
        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);

        public void update(EncodingStats newStats)
        {
            while (true)
            {
                EncodingStats current = stats.get();
                EncodingStats updated = current.mergeWith(newStats);
                if (stats.compareAndSet(current, updated))
                    return;
            }
        }

        public EncodingStats get()
        {
            return stats.get();
        }
    }
}
