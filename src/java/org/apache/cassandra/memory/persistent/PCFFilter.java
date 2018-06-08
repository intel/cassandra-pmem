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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;

import com.google.common.collect.ImmutableMap;

import lib.util.persistent.AnyPersistent;
//import lib.util.persistent.BitUtil;
import lib.util.persistent.PersistentByteArray;
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.PersistentString;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.MCFFilter;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

public class PCFFilter implements MCFFilter
{
    TableMetadata metadata;
    private Row.Builder builder;

    public PCFFilter(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    /*
    Read partitions with multiple row
     */
    @Override
    public Unfiltered getUnfilteredRowIterator(Map.Entry<MClusteringKey, MRow> persistentRowEntry,
                                               TableMetadata metadata)
    {
        builder = BTreeRow.sortedBuilder();
        ByteBuffer[] clusteringValues = new ByteBuffer[persistentRowEntry.getKey().getNumKeys()];
        //create new clustering info
        for (int i = 0; i < persistentRowEntry.getKey().getNumKeys(); i++)
        {
            clusteringValues[i] = ByteBuffer.wrap(persistentRowEntry.getKey().getValueBuf(i));
        }

        builder.newRow(Clustering.make(clusteringValues));
        PersistentRow persistentRow = (PersistentRow) persistentRowEntry.getValue();
        /* populate cells and add to row */
        PersistentByteArray pCells = persistentRow.getCells();
        ByteBuffer cells = ByteBuffer.wrap(pCells.toArray());

        long bitmap = cells.getLong();


        for (ColumnMetadata column : metadata.regularColumns())
        {
            if ((bitmap & 1L) == 0L)
            {

                bitmap = bitmap >> 1;
                continue;
            }
            int cellSize = cells.getInt();
            cells.limit(cells.position() + cellSize);
            ByteBuffer cellBuffer = cells.slice();
            long timestamp;
            int localDeletionTime = Cell.NO_DELETION_TIME;
            int ttl = Cell.NO_TTL;

            //  ColumnMetadata column = iter.getValue();
            if (column.isSimple())
            {

                int flag = cellBuffer.get();

                boolean isDeleted = (flag & Cell.PMCellWriter.IS_DELETED_MASK) != 0;
                boolean isExpiring = (flag & Cell.PMCellWriter.IS_EXPIRING_MASK) != 0;
                boolean useRowTTL = (flag & Cell.PMCellWriter.USE_ROW_TTL_MASK) != 0;
                boolean hasValue = (flag & Cell.PMCellWriter.HAS_EMPTY_VALUE_MASK) == 0;

                timestamp = cellBuffer.getInt();

                if ((isDeleted || isExpiring) && !useRowTTL)
                    localDeletionTime = cellBuffer.getInt();

                if (isExpiring && !useRowTTL)
                    ttl = cellBuffer.getInt();
                int length =0;
                if(hasValue)
                {
                    length = column.type.valueLengthIfFixed();
                    if (length < 0)
                        length = cellBuffer.getInt();
                }

                Cell cassCell;
                if (length > 0)
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              cellBuffer, null);
                else
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER, null);

                builder.addCell(cassCell);
            }

            else if (column.isComplex())
            {
                int complexCellCount = cells.getInt();

                for(int index =0;index < complexCellCount; index++)
                {
                    //int complexCellSize = cells.getInt();
                    cells.getInt(); //reads complex cell size
                    cells.limit(cells.position() + cellSize);
                    ByteBuffer complexCellBuffer = cells.slice();

                    localDeletionTime = Cell.NO_DELETION_TIME;
                    ttl = Cell.NO_TTL;

                    int flag = cellBuffer.get();

                    boolean isDeleted = (flag & Cell.PMCellWriter.IS_DELETED_MASK) != 0;
                    boolean isExpiring = (flag & Cell.PMCellWriter.IS_EXPIRING_MASK) != 0;
                    boolean useRowTTL = (flag & Cell.PMCellWriter.USE_ROW_TTL_MASK) != 0;

                    timestamp = cellBuffer.getInt();

                    if ((isDeleted || isExpiring) && !useRowTTL)
                        localDeletionTime = cellBuffer.getInt();

                    if (isExpiring && !useRowTTL)
                        ttl = cellBuffer.getInt();

                    cellBuffer.getInt();
                    Cell cassCell;
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                              CellPath.create(complexCellBuffer));
                    builder.addCell(cassCell);


                }
            }
            cells.limit(cells.capacity());
            cells.position(cells.position()+cellSize);

            bitmap = bitmap >> 1;
        }

        return builder.build();


    }

    /*
    Read partitions with single row
     */
    //TODO: Need to consolidate common functionality
    @Override
    public Unfiltered getUnfilteredRowSingle(MRow mRow, Clustering clustering, TableMetadata metadata)
    {
        builder = BTreeRow.sortedBuilder();
        if (clustering != null)
            builder.newRow(clustering);
        else
            builder.newRow(Clustering.EMPTY);


        /* populate cells and add to row */
        PersistentByteArray pCells = mRow.getCells();
        ByteBuffer cells = ByteBuffer.wrap(pCells.toArray());

        long bitmap = cells.getLong();

        for (ColumnMetadata column : metadata.regularColumns())
        {
            if ((bitmap & 1L) == 0L)
            {

                bitmap = bitmap >> 1;
                continue;
            }
            int cellSize = cells.getInt();
            cells.limit(cells.position()+cellSize);
            ByteBuffer cellBuffer = cells.slice();
            long timestamp;
            int localDeletionTime = Cell.NO_DELETION_TIME;
            int ttl = Cell.NO_TTL;

            if (column.isSimple())
            {

                int flag = cellBuffer.get();

                boolean isDeleted = (flag & Cell.PMCellWriter.IS_DELETED_MASK) != 0;
                boolean isExpiring = (flag & Cell.PMCellWriter.IS_EXPIRING_MASK) != 0;
                boolean useRowTTL = (flag & Cell.PMCellWriter.USE_ROW_TTL_MASK) != 0;
                boolean hasValue = (flag & Cell.PMCellWriter.HAS_EMPTY_VALUE_MASK) == 0;
                timestamp = cellBuffer.getInt();

                if ((isDeleted || isExpiring) && !useRowTTL)
                    localDeletionTime = cellBuffer.getInt();

                if(isExpiring && !useRowTTL)
                    ttl = cellBuffer.getInt();
                int length = 0;
                if(hasValue)
                {
                    length = column.type.valueLengthIfFixed();

                    if (length < 0)
                        length = cellBuffer.getInt();
                }


                Cell cassCell;
                if (length > 0)
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              cellBuffer, null);
                else
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER, null);

                builder.addCell(cassCell);
            }

            else if (column.isComplex())
            {
                int complexCellCount = cellBuffer.getInt();
                cellBuffer.position(cellBuffer.position()+16);

                for(int index =0;index < complexCellCount; index++)
                {

                    int complexCellSize = cellBuffer.getInt();
                    cellBuffer.limit(cellBuffer.position() + complexCellSize);
                    ByteBuffer complexCellBuffer = cellBuffer.slice();

                    localDeletionTime = Cell.NO_DELETION_TIME;
                    ttl = Cell.NO_TTL;

                    int flag = complexCellBuffer.get();

                    boolean isDeleted = (flag & Cell.PMCellWriter.IS_DELETED_MASK) != 0;
                    boolean isExpiring = (flag & Cell.PMCellWriter.IS_EXPIRING_MASK) != 0;
                    boolean useRowTTL = (flag & Cell.PMCellWriter.USE_ROW_TTL_MASK) != 0;

                    timestamp = complexCellBuffer.getInt();

                    if ((isDeleted || isExpiring) && !useRowTTL)
                        localDeletionTime = complexCellBuffer.getInt();

                    if (isExpiring && !useRowTTL)
                        ttl = complexCellBuffer.getInt();

                    complexCellBuffer.getInt();
                    Cell cassCell;
                    cassCell = new BufferCell(column, timestamp, ttl, localDeletionTime,
                                              ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                              CellPath.create(complexCellBuffer));
                    builder.addCell(cassCell);
                    cellBuffer.limit(cellBuffer.capacity());
                    cellBuffer.position(cellBuffer.position()+complexCellSize);
                }

            }
            cells.limit(cells.capacity());
            cells.position(cells.position()+cellSize);

            bitmap = bitmap >> 1;

        }
        return builder.build();

    }

    /*
    Filter based on ClusteringIndexNames
     */
    //TODO: Works for now. Need to improve & remove hard-coded stuff
    @Override
    public UnfilteredRowIterator getRowIterator(ClusteringIndexNamesFilter filter, Object persistentVal, DecoratedKey dKey)
    {

        NavigableSet<Clustering> clusteringSet = filter.requestedRows();
        Iterator clusteringIterator = clusteringSet.iterator();
        return new AbstractUnfilteredRowIterator(metadata,
                                                 dKey,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 false,
                                                 EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                while (clusteringIterator.hasNext())
                {
                    Clustering c = (Clustering) clusteringIterator.next();
                    PersistentClusteringKey pclKey = new PersistentClusteringKey();
                    for (int i=0;i<filter.getSlices(metadata).get(0).start().size();i++)
                    {
                        ByteBuffer slice = filter.getSlices(metadata).get(0).start().get(i);
                        pclKey.addClusteringKeyColumn( UTF8Type.instance,slice.array());
                    }
                    MRow pRow = (MRow) ((PersistentColumnFamilySortedMap) persistentVal).get(pclKey);
                    if (pRow != null)
                    {
                        return getUnfilteredRowSingle(pRow, c, metadata);
                    }
                }
                return endOfData();
            }
        };
    }

    /*
    Filter based on Slice information
     */
    //TODO: Works for now. Need to improve & remove hard-coded stuff
    @Override
    public UnfilteredRowIterator getSliceIterator(ClusteringIndexFilter filter, Iterator<Map.Entry<MClusteringKey, MRow>> iter,
                                                  TableMetadata metadata, MTableMetadata mTableMetadata, DecoratedKey decoratedKey)
    {
        Iterator<Map.Entry<MClusteringKey, MRow>> pcfMapIterator = iter;

        return new AbstractUnfilteredRowIterator(metadata,
                                                 decoratedKey,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 false,
                                                 EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                while (pcfMapIterator.hasNext())
                {

                    Map.Entry<MClusteringKey, MRow> persistentClusteringKeyMap = pcfMapIterator.next();
                    MClusteringKey persistentClusteringKey = persistentClusteringKeyMap.getKey();
                    MRow pRow = persistentClusteringKeyMap.getValue();
                    if (filter.getSlices(metadata).hasLowerBound())
                    {

                        PersistentClusteringKey startPersistentClusteringKey = new PersistentClusteringKey();
                        for (int i=0;i<filter.getSlices(metadata).get(0).start().size();i++)
                        {
                            ByteBuffer slice = filter.getSlices(metadata).get(0).start().get(i);
                            startPersistentClusteringKey.addClusteringKeyColumn( UTF8Type.instance, slice.array());
                        }
                        if (Arrays.equals(persistentClusteringKey.getValueBuf(0), startPersistentClusteringKey.getValueBuf(0)))
                        {
                            ByteBuffer[] clusteringValues = new ByteBuffer[persistentClusteringKey.getNumKeys()];
                            //create new clustering info
                            for (int i = 0; i < persistentClusteringKey.getNumKeys(); i++)
                            {
                                clusteringValues[i] = ByteBuffer.wrap(persistentClusteringKey.getValueBuf(i));
                            }
                            return getUnfilteredRowSingle(pRow, Clustering.make(clusteringValues), metadata);
                        }
                    }
                    else
                        return getUnfilteredRowSingle(pRow,
                                                      Clustering.make(ByteBuffer.wrap(persistentClusteringKey.getValueBuf(0))),
                                                      metadata);
                }
                return endOfData();
            }
        };
    }

    public UnfilteredRowIterator getUnfilteredIterator(ClusteringIndexFilter filter, PersistentColumnFamilySortedMap rowMap, TableMetadata metadata, MTableMetadata mTableMetadata, DecoratedKey decoratedKey)
    {
        Map<MClusteringKey, MRow> subRowMap ;
        if(filter.getSlices(metadata).size() == 1)
        {
            boolean includeStart = filter.getSlices(metadata).get(0).start().isInclusive();
            ByteBuffer slice;

            PersistentClusteringKey startPersistentClusteringKey = new PersistentClusteringKey();
            for (int i=0;i<filter.getSlices(metadata).get(0).start().size();i++)
            {
                slice = filter.getSlices(metadata).get(0).start().get(i);
                startPersistentClusteringKey.addClusteringKeyColumn( UTF8Type.instance, slice.array());
            }

            subRowMap = rowMap.tailMap( startPersistentClusteringKey, includeStart);
            return getSliceIterator(filter, subRowMap.entrySet().iterator(), metadata, mTableMetadata, decoratedKey);

        }
        else
        {
            //TODO: need to handle multiple slice iteration here
            subRowMap = rowMap;

            return getSliceIterator(filter, subRowMap.entrySet().iterator(), metadata, mTableMetadata, decoratedKey);
        }

    }

}
