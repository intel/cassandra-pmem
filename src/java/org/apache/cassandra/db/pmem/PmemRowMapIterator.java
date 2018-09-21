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
import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Raw;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import java.util.concurrent.FutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PmemRowMapIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator//Iterator<Unfiltered>
{
    protected final DataInputPlus in;
    protected final TableMetadata metadata;
    private final ClusteringPrefix.Deserializer clusteringDeserializer;
    private final Row.Builder builder;
    private final ARTree pmemRowTree;
    private final Heap heap;
    private final ARTree.EntryIterator pmemRowTreeIterator;
    private DecoratedKey dkey;
    private ColumnFilter filter;
    private ClusteringIndexFilter namesFilter = null;
    public FutureTask<Void> ack;
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMapIterator.class);

    private PmemRowMapIterator(TableMetadata metadata,
                               DataInputPlus in, long pmemRowMapTreeAddr, Heap heap, DecoratedKey key, ColumnFilter filter)
    {
        this.metadata = metadata;
        this.in = in;
        this.heap = heap;
        SerializationHeader header = SerializationHeader.makeWithoutStats(metadata);
        this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
        this.builder = BTreeRow.sortedBuilder();
        this.pmemRowTree = new ARTree(heap,pmemRowMapTreeAddr);
        this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();
        this.dkey = key; //TODO: need to read from persistent
        this.filter =filter;
        this.ack = new FutureTask<Void>(()->null);
    }

    private PmemRowMapIterator(TableMetadata metadata,
                               DataInputPlus in, long pmemRowMapTreeAddr, Heap heap, DecoratedKey key, ColumnFilter filter,ClusteringIndexFilter namesFilter)
    {
        this.metadata = metadata;
        this.in = in;
        this.heap = heap;
        SerializationHeader header = SerializationHeader.makeWithoutStats(metadata);
        this.clusteringDeserializer = new ClusteringPrefix.Deserializer(metadata.comparator, in, header);
        this.builder = BTreeRow.sortedBuilder();
        this.pmemRowTree = new ARTree(heap,pmemRowMapTreeAddr);
        this.pmemRowTreeIterator = pmemRowTree.getEntryIterator();
        this.dkey = key; //TODO: need to read from persistent
        this.filter =filter;
        this.namesFilter = namesFilter;
        this.ack = new FutureTask<Void>(()->null);
    }

    public static UnfilteredRowIterator create(TableMetadata tableMetadata, MemoryBlockDataInputPlus inputPlus, long pmemRowMapTreeAddr, Heap heap, DecoratedKey key, ColumnFilter filter, ClusteringIndexFilter namesFilter)
    {
        return new PmemRowMapIterator(tableMetadata, inputPlus, pmemRowMapTreeAddr, heap, key, filter,namesFilter);
    }
    public static UnfilteredRowIterator create(TableMetadata metadata,
                                               MemoryBlockDataInputPlus in, long pmemRowMapTreeAddr, Heap heap, DecoratedKey key, ColumnFilter filter)//,SerializationHeader header,SerializationHelper helper)
    {
        return new PmemRowMapIterator(metadata, in, pmemRowMapTreeAddr, heap, key, filter);
    }

    protected Unfiltered computeNext() //TODO: This is a roundabout way. Revisit
    {
        while (pmemRowTreeIterator.hasNext())
        {
            ARTree.Entry nextEntry = pmemRowTreeIterator.next();
            if (nextEntry == null)
                return endOfData();
            ByteBuffer clusteringbuffer = ByteBuffer.wrap(nextEntry.getKey());
            Clustering clustering = Clustering.serializer.deserialize(clusteringbuffer, -1, metadata.comparator.subtypes());
            if((namesFilter != null) && (namesFilter instanceof ClusteringIndexSliceFilter)) //Not the slice we are looking for
            {
                boolean flag = ((ClusteringIndexSliceFilter) namesFilter).requestedSlices().selects(clustering);
                if(flag == false)
                    continue;
            }
            SerializationHeader serializationHeader = SerializationHeader.makeWithoutStats(metadata);
          //  Columns headerColumns = serializationHeader.columns(false);
            SerializationHelper helper = new SerializationHelper(metadata, -1, SerializationHelper.Flag.LOCAL, filter);
            long timestamp = FBUtilities.nowInSeconds(); //TODO: Hope this works for now.FIX THIS
            int ttl = LivenessInfo.NO_TTL;
            int localDeletionTime = LivenessInfo.NO_EXPIRATION_TIME;
            LivenessInfo rowLiveness = LivenessInfo.withExpirationTime(timestamp, ttl, localDeletionTime);
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(clustering);
            Columns regulars = null;
            MemoryBlock cellMemoryRegion = heap.memoryBlockFromAddress(Raw.class, nextEntry.getValue());
            MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(cellMemoryRegion, heap); //TODO: Pass the heap as parameter for now until memoryblock.copyfromArray is sorted out
            try
            {
                regulars = Columns.serializer.deserializeSubset(filter.fetchedColumns().regulars, memoryBlockDataInputPlus);
            }
            catch (IOException e)
            {
                logger.error(e.getMessage(), e);//TODO: Refine exception handling
                e.printStackTrace();
            }
            if((namesFilter != null) && (namesFilter instanceof ClusteringIndexNamesFilter) && (((ClusteringIndexNamesFilter)namesFilter).requestedRows().first().size() >0))//Filter on few columns
            {
                if (namesFilter.selects(clustering))
                {
                    try
                    {
                        for (ColumnMetadata column : regulars)
                        {
                            if (filter.queriedColumns().contains(column))
                            {

                                if (column.isSimple())
                                    readSimpleColumn(column, memoryBlockDataInputPlus, serializationHeader, helper, builder, rowLiveness);
                                else
                                    readComplexColumn(column, memoryBlockDataInputPlus, serializationHeader, helper, false, builder, rowLiveness); //TODO: address hasComplexDeletion
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.error(e.getMessage(), e);//TODO: Refine exception handling
                        e.printStackTrace();
                    }
                }
                else
                {
                    continue;
                }
            }
            else
            {
                for (ColumnMetadata column : regulars)
                {
                    if (column.isSimple())
                        readSimpleColumn(column, memoryBlockDataInputPlus, serializationHeader, helper, builder, rowLiveness);
                    else
                        readComplexColumn(column, memoryBlockDataInputPlus, serializationHeader, helper, false, builder, rowLiveness); //TODO: address hasComplexDeletion
                }
            }
            return builder.build();
        }
        ack.run();
        return endOfData();
    }

    private void readComplexColumn(ColumnMetadata column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, boolean hasComplexDeletion, Row.Builder builder, LivenessInfo rowLiveness)
    {
        try
        {
            if (helper.includes(column))
            {
                helper.startOfComplexColumn(column);
                if (hasComplexDeletion)
                {
                    DeletionTime complexDeletion = header.readDeletionTime(in);
                    if (!helper.isDroppedComplexDeletion(complexDeletion))
                        builder.addComplexDeletion(column, complexDeletion);
                }
                int count = (int) in.readUnsignedVInt();
                while (--count >= 0)
                {
                    Cell cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
                    if (helper.includes(cell, rowLiveness) && !helper.isDropped(cell, true))
                        builder.addCell(cell);
                }
                helper.endOfComplexColumn();
            }
            else
            {
                skipComplexColumn(in, column, header, hasComplexDeletion);
            }
        }
        catch(IOException e)
        {
            logger.error(e.getMessage(), e);//TODO: Refine exception handling
            e.printStackTrace();
        }
    }

    private void readSimpleColumn(ColumnMetadata column, DataInputPlus in, SerializationHeader header, SerializationHelper helper, Row.Builder builder, LivenessInfo rowLiveness)
    {
        try
        {
            if (helper.includes(column))
            {
                Cell cell = Cell.serializer.deserialize(in, rowLiveness, column, header, helper);
                if (helper.includes(cell, rowLiveness) && !helper.isDropped(cell, false))
                    builder.addCell(cell);
            }
            else
            {
                Cell.serializer.skip(in, column, header);
            }
        }
        catch(IOException e)
        {
            logger.error(e.getMessage(), e);//TODO: Refine exception handling
            e.printStackTrace();
        }
    }

    private void skipComplexColumn(DataInputPlus in, ColumnMetadata column, SerializationHeader header, boolean hasComplexDeletion)
    throws IOException
    {
        if (hasComplexDeletion)
            header.skipDeletionTime(in);
        int count = (int) in.readUnsignedVInt();
        while (--count >= 0)
            Cell.serializer.skip(in, column, header);
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
        return DeletionTime.LIVE;
    }

    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }
}
