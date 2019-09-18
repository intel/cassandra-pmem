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
import lib.llpl.TransactionalHeap;
import lib.llpl.TransactionalMemoryBlock;
//import lib.llpl.Raw;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import java.util.concurrent.FutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PmemRowMapIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    protected final TableMetadata metadata;
   // private final ClusteringPrefix.Deserializer clusteringDeserializer;
    //private final Row.Builder builder;
    private final ARTree pmemRowTree;
    private final TransactionalHeap heap;
    private final ARTree.EntryIterator pmemRowTreeIterator;
    private DecoratedKey dkey;
    private ColumnFilter filter;
    private ClusteringIndexFilter namesFilter = null;
    public FutureTask<Void> ack;
    SerializationHeader header;
    DeletionTime deletionTime = DeletionTime.LIVE;
    DataRange dataRange;

    private static final Logger logger = LoggerFactory.getLogger(PmemRowMapIterator.class);

    private PmemRowMapIterator(TableMetadata metadata,
                               DataInputPlus in, long pmemRowMapTreeAddr, TransactionalHeap heap, DecoratedKey key, ColumnFilter filter,ClusteringIndexFilter namesFilter, DeletionTime deletionTime, DataRange dataRange)
    {
        this.metadata = metadata;
        this.heap = heap;
        this.header =  SerializationHeader.makeWithoutStats(metadata);
     //   this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
       // this.builder = BTreeRow.sortedBuilder();
        this.pmemRowTree = new ARTree(heap,pmemRowMapTreeAddr);

        this.dkey = key; //TODO: need to read from persistent
        this.filter =filter;
        this.namesFilter = namesFilter;
        this.deletionTime = deletionTime;
        this.ack = new FutureTask<Void>(()->null);
        this.dataRange = dataRange;

        if(dataRange != null) //TODO: This needs to be moved to a better place & some serious renaming needed & refactored
        {
            ClusteringIndexFilter clusteringIndexFilter = dataRange.clusteringIndexFilter(key);
            if ( clusteringIndexFilter != null && clusteringIndexFilter.getSlices(metadata) != Slices.ALL) //Read slices
            {
                Slices slices = clusteringIndexFilter.getSlices(metadata);
                if (slices.size() == 1)
                {
                    Slice slice = slices.get(0);
                    boolean includeStart = clusteringIndexFilter.getSlices(metadata).get(0).start().isInclusive();
                    boolean includeEnd = clusteringIndexFilter.getSlices(metadata).get(0).end().isInclusive();
                    ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
                    ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();
                    Clustering clusteringStart;
                    Clustering clusteringEnd;
                    ByteBuffer clusteringBufferStart;
                    ByteBuffer clusteringBufferEnd;


                    if ((start != null && start.size() != 0) && (end != null && end.size() != 0))//TODO:
                    {
                        clusteringStart = Clustering.make(start.getRawValues());
                        clusteringBufferStart = Clustering.serializer.serialize(clusteringStart, -1, metadata.comparator.subtypes());
                        clusteringEnd = Clustering.make(start.getRawValues());
                        clusteringBufferEnd = Clustering.serializer.serialize(clusteringEnd, -1, metadata.comparator.subtypes());
                        this.pmemRowTreeIterator = pmemRowTree.getEntryIterator(clusteringBufferStart.array(), includeStart, clusteringBufferEnd.array(), includeEnd);
                    }
                    else if ((start != null) && (start.size() != 0))
                    {
                        Clustering clustering;
                        clustering = Clustering.make(start.getRawValues());
                        clusteringBufferStart = Clustering.serializer.serialize(clustering, -1, metadata.comparator.subtypes());
                        this.pmemRowTreeIterator = pmemRowTree.getEntryIterator(clusteringBufferStart.array(), includeStart);
                    }
                    else
                        this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();

                }
                else //TODO: Need to handle multiple slices
                    this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();
            }
            else
                this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();
        }
        else
        {
            this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();
        }
   }

    public static UnfilteredRowIterator create(TableMetadata tableMetadata, MemoryBlockDataInputPlus inputPlus, long pmemRowMapTreeAddr, TransactionalHeap heap, DecoratedKey key, ColumnFilter filter, ClusteringIndexFilter namesFilter, DeletionTime deletionTime)
    {
        return new PmemRowMapIterator(tableMetadata, inputPlus, pmemRowMapTreeAddr, heap, key, filter,namesFilter, deletionTime, null);
    }
    public static UnfilteredRowIterator create(TableMetadata metadata,
                                               MemoryBlockDataInputPlus in, long pmemRowMapTreeAddr, TransactionalHeap heap, DecoratedKey key, ColumnFilter filter, DeletionTime deletionTime)//,SerializationHeader header,SerializationHelper helper)
    {

        return new PmemRowMapIterator(metadata, in, pmemRowMapTreeAddr, heap, key, filter, null,deletionTime, null);
    }

    public static UnfilteredRowIterator create(TableMetadata metadata,
                                               MemoryBlockDataInputPlus in, long pmemRowMapTreeAddr, TransactionalHeap heap, DecoratedKey key, ColumnFilter filter, DeletionTime deletionTime, DataRange dataRange)//,SerializationHeader header,SerializationHelper helper)
    {

        return new PmemRowMapIterator(metadata, in, pmemRowMapTreeAddr, heap, key, filter, null,deletionTime, dataRange);
    }

    protected Unfiltered computeNext() //TODO: This is a roundabout way. Revisit
    {
        Row.Builder builder = BTreeRow.sortedBuilder();

        while (pmemRowTreeIterator.hasNext())
        {
            ARTree.Entry nextEntry = pmemRowTreeIterator.next();
            if (nextEntry == null)
            {
     //           ack.run();
                return endOfData();
            }
            ByteBuffer clusteringbuffer = ByteBuffer.wrap(nextEntry.getKey());
            Clustering clustering = Clustering.serializer.deserialize(clusteringbuffer, -1, metadata.comparator.subtypes());
            if ((namesFilter != null) && (namesFilter instanceof ClusteringIndexSliceFilter)) //Not the slice we are looking for
            {
                boolean flag = ((ClusteringIndexSliceFilter) namesFilter).requestedSlices().selects(clustering);
                if (flag == false)
                    continue;
            }

            SerializationHeader serializationHeader ;
            SerializationHelper helper = new SerializationHelper(metadata, -1, SerializationHelper.Flag.LOCAL);//TODO: Check if version needs to be set
            TransactionalMemoryBlock cellMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
            MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(cellMemoryBlock, heap);

            int flags = cellMemoryBlock.getByte(0);
            if((TablesManager.tablesMetaDataMap.size() > 0) && (TablesManager.tablesMetaDataMap.get(metadata.id).size() > 1))
            {
                if (PmemRowSerializer.isTableAltered(flags))
                {
                    //byte versionSize = cellMemoryBlock.getByte(1);
                    int version = cellMemoryBlock.getByte(2);
                    serializationHeader = TablesManager.tablesMetaDataMap.get(metadata.id).get(version-1);
                }
                else
                {
                    serializationHeader = TablesManager.tablesMetaDataMap.get(metadata.id).get(0);
                }
            }
            else
            {
                    serializationHeader = new SerializationHeader(false,
                                                              metadata,
                                                              metadata.regularAndStaticColumns(),
                                                              EncodingStats.NO_STATS);
            }
            try
            {
                builder.newRow(clustering);

                Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
                if((namesFilter != null) && (namesFilter instanceof ClusteringIndexNamesFilter) && (((ClusteringIndexNamesFilter)namesFilter).requestedRows().first().size() >0))
                {
                    if (namesFilter.selects(clustering))
                    {
                        return unfiltered;
                    }
                    else
                        continue;
                }
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                ack.cancel(true);
                throw new IOError(e);
            }
        }
  //      ack.run();
        return endOfData();
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public boolean isReverseOrder()
    {
        return false;
    }

    public RegularAndStaticColumns columns()
    {
        return metadata().regularAndStaticColumns();
    }

    public DecoratedKey partitionKey()
    {
        return dkey;
    }

    public Row staticRow()
    {

        return Rows.EMPTY_STATIC_ROW;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return this.deletionTime;
    }

    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    @Override
    public void close()
    {
        ack.run();
    }
}
