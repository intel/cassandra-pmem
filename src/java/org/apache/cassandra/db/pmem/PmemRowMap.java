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

import java.io.IOException;
import java.nio.ByteBuffer;
import com.google.common.collect.Collections2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Raw;
import lib.llpl.Transaction;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

// keys are Clusterings, and are stored in a MemoryRegion seperate from values
// values are Rows, with the columns serialized (with assocaited header and column information) stored contiguously
// in one MemoryRegion
// -- EVENTUALLY, that's how it wil be stored. I've naively just invoked Cell.serializer.serialize for the cells ...


// basically, this is a sorted hash map, persistent and unsafe, specific to a given partition
public class PmemRowMap
{
    private  MemoryBlock block;
    private static Heap heap;
    private final ARTree arTree;
    private TableMetadata tableMetadata;
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMap.class);

    public PmemRowMap(Heap heap,  ARTree arTree,TableMetadata tableMetadata)
    {
        this.heap = heap;
        this.arTree = arTree;
        this.tableMetadata = tableMetadata;
    }

    public static PmemRowMap create(Heap heap, TableMetadata tableMetadata,Transaction tx)
    {
        PmemRowMap[] map = new PmemRowMap[1];
        tx.execute(() -> { //TODO: this transaction not needed. revisit
     //       int headerSize = 8;
            ARTree arTree = new ARTree(heap);
            map[0] = new PmemRowMap(heap,  arTree, tableMetadata);
        });
        return map[0];
    }

    public static PmemRowMap loadFromAddress(Heap heap,long address,TableMetadata tableMetadata)
    {
        PmemRowMap[] map = new PmemRowMap[1];
        ARTree arTree = new ARTree(heap, address);
        map[0] = new PmemRowMap(heap, arTree, tableMetadata );
        return map[0];
    }

    public long getAddress()
    {
        return arTree.address();
    }

    public   Row getMergedRow(Row newRow, Long mb)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        Clustering clustering = newRow.clustering();

        MemoryBlock oldBlock = heap.memoryBlockFromAddress(Raw.class, mb);
        MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(oldBlock, heap); //TODO: Pass the heap as parameter for now until memoryblock.copyfromArray is sorted out
        SerializationHeader serializationHeader = SerializationHeader.makeWithoutStats(tableMetadata);
//        Columns headerColumns = serializationHeader.columns(false);
        SerializationHelper helper = new SerializationHelper(tableMetadata, -1, SerializationHelper.Flag.LOCAL, null);
        long timestamp = FBUtilities.nowInSeconds(); //TODO: Revisit this
        int ttl = LivenessInfo.NO_TTL;
        int localDeletionTime = LivenessInfo.NO_EXPIRATION_TIME;
        LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(timestamp, ttl, localDeletionTime);
        builder.newRow(clustering);
        Columns regulars = null;
        try
        {
            regulars = Columns.serializer.deserializeSubset(tableMetadata.regularColumns(), memoryBlockDataInputPlus);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);//TODO: Refine exception handling
            e.printStackTrace();
        }
        try
        {
            for (ColumnMetadata column : regulars)
            {
                if (regulars.contains(column))
                {
                    Cell cell;
                    try
                    {
                        if (column.isSimple())
                        {
                            cell = Cell.serializer.deserialize(memoryBlockDataInputPlus, rowLiveness, column, serializationHeader, helper);
                            builder.addCell(cell);
                        }
                        else
                        {
                            int count = (int) memoryBlockDataInputPlus.readUnsignedVInt();
                            while (--count >= 0)
                            {
                                cell = Cell.serializer.deserialize(memoryBlockDataInputPlus, rowLiveness, column, serializationHeader, helper);
                                builder.addCell(cell);
                            }
                        }
                    }
                    catch (IndexOutOfBoundsException e)
                    {
                        logger.error(e.getMessage(), e);//TODO: Refine exception handling
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.error(e.getMessage(), e);//TODO: Refine exception handling
            e.printStackTrace();
        }
        Row currentRow = builder.build();
        return(Rows.merge(currentRow, newRow, FBUtilities.nowInSeconds()));
    }

    Long merge(Object newRow, Long mb)
    {
        Row row = (Row)newRow;
        MemoryBlock oldBlock = null;
        boolean reuseblock = false;
        if(mb !=0) //Get the existing row to merge with update
        {
            oldBlock = heap.memoryBlockFromAddress(Raw.class, mb);
            row = getMergedRow((Row)newRow, mb);
        }
        int size = 0;
        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        SerializationHeader serializationHeader = SerializationHeader.makeWithoutStats(tableMetadata);
        Columns headerColumns = serializationHeader.columns(false);
        //calculate size of memory block to create
        size += Columns.serializer.serializedSubsetSize(Collections2.transform(row, ColumnData::column), headerColumns);
        for (ColumnData data : row)
        {
            ColumnMetadata column = data.column();
            if (column.isSimple())
                size += Cell.serializer.serializedSize((Cell) data, column, pkLiveness, serializationHeader);
            else
                size += sizeOfComplexColumn((ComplexColumnData) data, column, row.hasComplexDeletion(), pkLiveness, serializationHeader);
        }
        MemoryBlock cellMemoryRegion;
        if(mb == 0)
            cellMemoryRegion = heap.allocateMemoryBlock(Raw.class,size);
        else if(oldBlock.size() < size)
        {
            cellMemoryRegion = heap.allocateMemoryBlock(Raw.class,size);
            heap.freeMemoryBlock(oldBlock);
        }
        else
        {
            cellMemoryRegion = oldBlock;//TODO: should we zero out if size is greater ?
            cellMemoryRegion.addToTransaction(0, cellMemoryRegion.size());
            reuseblock = true;
        }
        MemoryBlockDataOutputPlus cellsOutputPlus = new MemoryBlockDataOutputPlus(cellMemoryRegion, 0);
        try
        {
            Columns.serializer.serializeSubset(Collections2.transform(row, ColumnData::column), headerColumns, cellsOutputPlus);
        }
        catch (IOException e)
        {
            logger.error(e.getMessage(), e);//TODO: Refine exception handling
            e.printStackTrace();
        }
        for (ColumnData cd : row)
        {
            try
            {
                if (cd.column().isSimple())
                    Cell.serializer.serialize((Cell)cd, cd.column(), cellsOutputPlus, pkLiveness, serializationHeader);//((PartitionUpdate)update).metadata().getColumn(c.column().name), cellsOutputPlus, pkLiveness, serializationHeader);
                else
                    writeComplexColumn((ComplexColumnData) cd, cd.column(), false, pkLiveness, serializationHeader, cellsOutputPlus);
            }
            catch (IOException e)
            {
                logger.error(e.getMessage(), e);//TODO: Refine exception handling
                e.printStackTrace();
            }
        }
        long rowAddress = cellMemoryRegion.address();
        //flush
        if(!reuseblock)
        {
            cellMemoryRegion.flush(0, cellMemoryRegion.size());
        }
        return rowAddress;
    };

    private static void writeComplexColumn(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header, DataOutputPlus out)
    throws IOException
    {
        if (hasComplexDeletion)
            header.writeDeletionTime(data.complexDeletion(), out);
        out.writeUnsignedVInt(data.cellsCount());
        for (Cell cell : data)
            Cell.serializer.serialize(cell, column, out, rowLiveness, header);
    }

    private static long sizeOfComplexColumn(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion, LivenessInfo rowLiveness, SerializationHeader header)
    {
        long size = 0;
        if (hasComplexDeletion)
            size += header.deletionTimeSerializedSize(data.complexDeletion());
        size += TypeSizes.sizeofUnsignedVInt(data.cellsCount());
        for (Cell cell : data)
            size += Cell.serializer.serializedSize(cell, column, rowLiveness, header);
        return size;
    }

    public void put(Row row, PartitionUpdate update, Transaction tx) //throws IOException
    {
        /// KEY
        Clustering clustering = row.clustering();
        ByteBuffer clusteringBuffer;
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
}
