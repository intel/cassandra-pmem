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
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.primitives.Longs;

import lib.util.persistent.AnyPersistent;
//import lib.util.persistent.BitUtil;
import lib.util.persistent.PersistentByteArray;
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.PersistentString;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MHeader;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MStorageWrapper;
import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableWriter;
import org.apache.cassandra.memory.MTablesManager;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.memory.MUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;

public class PMTableWriter implements MTableWriter
{
    public MHeader mHeader;
    private MStorageWrapper mStorageWrapper;


    public PMTableWriter(MHeader mHeader, MStorageWrapper mStorageWrapper)
    {
        this.mHeader = mHeader;
        this.mStorageWrapper = mStorageWrapper;

    }

    private static boolean hasExtendedFlags(Row row)
    {
        return row.isStatic() || row.deletion().isShadowable();
    }

    @Override
    public void write(UnfilteredRowIterator iterator)
    {
        writePartition(iterator);
    }

    private void writePartition(UnfilteredRowIterator iterator)
    {
        MTablesManager pmTablesManager = mStorageWrapper.getMTablesManager(iterator.metadata().keyspace);
        assert pmTablesManager != null : "table manager instance is null";
        MTable pmTable = pmTablesManager.getMTable(iterator.metadata().id.asUUID());
        assert pmTable != null : "pmtable instance is null";

        byte[] parKey = new byte[iterator.partitionKey().getKey().remaining()];
        iterator.partitionKey().getKey().duplicate().get(parKey);
        MToken token = PMToken.getInstance(iterator.partitionKey().getToken().getTokenValue());
        MDecoratedKey persistentDecoratedKey = new PersistentDecoratedKey(parKey, token);
        MiscDomainData domainData = new MiscDomainData();
        domainData.mTable = (PMTable) pmTable;
        domainData.partitionKey = persistentDecoratedKey;
        if (pmTable.doesClusteringKeyExist())
        {
            pmTable.putPartitionKeyIfAbsent(persistentDecoratedKey);
            domainData.cfMap = pmTable.get(persistentDecoratedKey);
        }

        // Write rows
        while (iterator.hasNext())
        {
            Unfiltered iter = iterator.next();
            writeRow(iter, iterator.metadata(), domainData,iterator.stats());
        }
    }

    private void writeRow(Unfiltered unfiltered, TableMetadata tableMetadata,MiscDomainData domainData, EncodingStats stats)
    {
        int flags = 0;
        int extendedFlags = 0;
        if (unfiltered.kind().equals(Unfiltered.Kind.RANGE_TOMBSTONE_MARKER))
            return;
        Row row = (Row) unfiltered;
        boolean isStatic = row.isStatic();
        Columns headerColumns = mHeader.columns(isStatic);
        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        Row.Deletion deletion = row.deletion();
        boolean hasComplexDeletion = row.hasComplexDeletion();
        boolean hasAllColumns = (row.size() == headerColumns.size());
        boolean hasExtendedFlags = hasExtendedFlags(row);

        if (isStatic)
            extendedFlags |= MTableWriter.IS_STATIC;

        if (!pkLiveness.isEmpty())
            flags |= HAS_TIMESTAMP;
        if (pkLiveness.isExpiring())
            flags |= HAS_TTL;
        if (!deletion.isLive())
        {
            flags |= HAS_DELETION;
            if (deletion.isShadowable())
                extendedFlags |= HAS_SHADOWABLE_DELETION;
        }
        if (hasComplexDeletion)
            flags |= HAS_COMPLEX_DELETION;
        if (hasAllColumns)
            flags |= HAS_ALL_COLUMNS;

        if (hasExtendedFlags)
            flags |= EXTENSION_FLAG;

        MClusteringKey clusteringKey = null;
        MRow mRow;
        // Prepare clustering key if exists
        boolean isClusteringKeyAvailable = domainData.mTable.doesClusteringKeyExist();
        if (isClusteringKeyAvailable)
        {
            int clusteringSize = row.clustering().size();
            if (!isStatic && clusteringSize != 0)
            {
                clusteringKey = new PersistentClusteringKey();
                assert clusteringKey != null : "clustering key is null";
                for (int i = 0; i < clusteringSize; i++)
                {
                    byte[] clusKey = new byte[row.clustering().get(i).remaining()];
                    row.clustering().get(i).duplicate().get(clusKey);
                    clusteringKey.addClusteringKeyColumn(mHeader.clusteringTypes().get(i), clusKey);
                }
            }


            mRow = new PersistentRow((byte) flags, (byte) extendedFlags,
                                     !pkLiveness.isEmpty(), pkLiveness.timestamp(),
                                     pkLiveness.isExpiring(), pkLiveness.ttl(),
                                     pkLiveness.localExpirationTime(), !deletion.isLive(),
                                     new PersistentDeletionTime(deletion.time().markedForDeleteAt(),
                                                                deletion.time().localDeletionTime()));
        }
        else
        {
            mRow = new PersistentRowSingle((byte) flags, (byte) extendedFlags,
                                           !pkLiveness.isEmpty(), pkLiveness.timestamp(),
                                           pkLiveness.isExpiring(), pkLiveness.ttl(),
                                           pkLiveness.localExpirationTime(), !deletion.isLive(),
                                           new PersistentDeletionTime(deletion.time().markedForDeleteAt(),
                                                                      deletion.time().localDeletionTime()));
        }
        assert mRow != null : "mRow is null";
        PersistentRow existingRow = isClusteringKeyAvailable ? (PersistentRow) domainData.cfMap.get(clusteringKey)
                                                             : ((PersistentRowSingle) domainData.mTable.get(domainData.partitionKey)); // merge rows

        writeCell(row, flags, mRow, existingRow, stats, tableMetadata);
        if (existingRow == null)
        {
            if (isClusteringKeyAvailable)
                domainData.cfMap.put((PersistentClusteringKey) clusteringKey, (PersistentRow) mRow); // update CF in-place
            else
                domainData.mTable.getPMTable().putIfAbsent((PersistentDecoratedKey) domainData.partitionKey, (AnyPersistent) mRow);
        }
        // TODO:Address this later
        else

            updateRow(existingRow, mRow);
    }

    private void updateRow(PersistentRow existingRow, MRow updaterRow)
    {
        PersistentRowMetadata rowMetadata = existingRow.getMetadata();
        PersistentRowMetadata updateRowMetadata = ((PersistentRow) updaterRow).getMetadata();
     //   existingRow.getCells().putAll(((PersistentRow) updaterRow).getCells());
        boolean updateTimestamp = rowMetadata.getPrimaryKeyLivenessTimestamp() <= updateRowMetadata.getPrimaryKeyLivenessTimestamp();
        long pkTimestamp;
        int pkLocalExpTime;
        if (updateTimestamp)
        {
            pkTimestamp = updateRowMetadata.getPrimaryKeyLivenessTimestamp();
            pkLocalExpTime = updateRowMetadata.getPkLocalExpirationTime();
        }
        else
        {
            pkTimestamp = rowMetadata.getPrimaryKeyLivenessTimestamp();
            pkLocalExpTime = rowMetadata.getPkLocalExpirationTime();
        }
        MDeletionTime existingDelTime = rowMetadata.getDeletionTime();
        MDeletionTime updateDelTime = updateRowMetadata.getDeletionTime();
        if (existingDelTime != null && updateDelTime != null)
        {
            boolean updateDeletionTime = existingDelTime.markedForDeleteAt() < updateDelTime.markedForDeleteAt()
                                         || (existingDelTime.markedForDeleteAt() == updateDelTime.markedForDeleteAt()
                                             && existingDelTime.localDeletionTime() < updateDelTime.localDeletionTime());
            MDeletionTime deletionTime = updateDeletionTime ? updateDelTime : existingDelTime;
            if (pkTimestamp <= deletionTime.markedForDeleteAt())
            {
                pkTimestamp = Long.MIN_VALUE;
                pkLocalExpTime = Integer.MAX_VALUE;
            }
            if (((((int) rowMetadata.getExtendedFlags()) & HAS_SHADOWABLE_DELETION) != 0) && pkTimestamp > deletionTime.markedForDeleteAt())
            {
                deletionTime = new PersistentDeletionTime(Long.MIN_VALUE, Integer.MAX_VALUE);
            }
            rowMetadata.setDeletionTime((PersistentDeletionTime) deletionTime);
        }
        rowMetadata.setPrimaryKeyLivenessTimestamp(pkTimestamp);
        rowMetadata.setPkLocalExpirationTime(pkLocalExpTime);
    }

    @Inline
    private void writeCell(Row row, int flags, MRow mRow, PersistentRow existingRow, EncodingStats stats, TableMetadata tableMetadata)
    {
        boolean isStatic = row.isStatic();
        Columns headerColumns = mHeader.columns(isStatic);// mHeader.columnsCollector.get();
        SearchIterator<ColumnMetadata, ColumnMetadata> si = headerColumns.iterator();
        // PersistentSkipListMap<PersistentString, AnyPersistent> existingCells = null;
        // PersistentByteArray existingCells= null;
       // byte[] columnsBitMapArray = new byte[8];

        LivenessInfo pkLiveness = row.primaryKeyLivenessInfo();
        Iterator<ColumnData> cellIterator = row.iterator();
	    byte[][] cells = new byte[row.columns().size()][];
	    int cellIndex = 0;
	    int totalCellSize = 0;
	    long columnsBitMap = Columns.Serializer.encodeBitmap(row.columns(), tableMetadata.regularAndStaticColumns().columns(isStatic),tableMetadata.regularAndStaticColumns().size() );
        byte[] columnsBitMapArray = Longs.toByteArray(columnsBitMap ^ ((1 << tableMetadata.regularAndStaticColumns().size()) -1));

        while (cellIterator.hasNext())
        {

            ColumnData cd = cellIterator.next();
            ColumnMetadata column = si.next(cd.column());
            assert column != null : cd.column().toString();

            if (cd.column().isSimple())
            {
                cells[cellIndex] = Cell.cellWriter.writeCelltoMemory((Cell) cd, column, pkLiveness, null, stats);
                if(cells[cellIndex] != null)
                    totalCellSize += cells[cellIndex].length;
                cellIndex++;

            }
            else
            {
                cells[cellIndex] = writeComplexColumnToMemory((ComplexColumnData) cd,
                                                                column,
                                                                (flags & HAS_COMPLEX_DELETION) != 0,
                                                                pkLiveness, stats);
                if(cells[cellIndex] != null)
                    totalCellSize += cells[cellIndex].length;
                cellIndex++;
            }
        }

        // Write all cells at once
        //if (!cells.isEmpty())
        if(existingRow == null)
            ((PersistentRow) mRow).addCells(columnsBitMapArray, cells, totalCellSize);
        else
        {
            int noColumns = tableMetadata.regularColumns().size();
            ((PersistentRow) existingRow).updateCells(columnsBitMapArray, cells, totalCellSize,noColumns);
        }
    }

    private byte[] writeComplexColumnToMemory(ComplexColumnData data, ColumnMetadata column, boolean hasComplexDeletion,
                                                             LivenessInfo rowLiveness, EncodingStats stats)
    {

        int complexCellIndex = 0;
        int totalCellSize = 0;
        byte[][] cells = new byte[data.cellsCount()][];
        for (Cell cell : data)
        {
            cells[complexCellIndex] = Cell.cellWriter.writeCelltoMemory(cell, column, rowLiveness, null, stats);
            if(cells[complexCellIndex] != null)
                totalCellSize += cells[complexCellIndex].length;
            complexCellIndex++;

        }

        ByteBuffer complexCell = ByteBuffer.allocate(4+4 + 12 + 4 + totalCellSize);    // 4 complex cell count, for 12 for DeletionTime (8 + 4), 4 for CellType
        complexCell.putInt(totalCellSize+20);
        complexCell.putInt(complexCellIndex);
        complexCell.putLong(data.complexDeletion().markedForDeleteAt());
        complexCell.putInt(data.complexDeletion().localDeletionTime());
        complexCell.putInt(MUtils.getPMDataType(mHeader.getType(column), mHeader.getType(column).isMultiCell()));
        for (int i = 0; i < cells.length; i++) {
            complexCell.put(cells[i]);
        }
        return complexCell.array();
    }
}

class MiscDomainData
{
    public PersistentColumnFamilySortedMap cfMap;
    public PMTable mTable;
    public MDecoratedKey partitionKey;

}
