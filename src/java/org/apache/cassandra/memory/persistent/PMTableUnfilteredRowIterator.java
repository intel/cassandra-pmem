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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;

import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTableUnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

/* An iterator for PersistentColumnFamilySortedMap */

public class PMTableUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements MTableUnfilteredRowIterator
{

    PCFFilter filter;
    private DecoratedKey dKey;
    private TableMetadata metadata;
    private MTableMetadata persistentTableMetadata;
    private PersistentColumnFamilySortedMap pcfMap;
    private PersistentRowSingle pRow;
    private Iterator<PersistentRow> pcfRowIterator;
    private Iterator<Map.Entry<MClusteringKey, MRow>> pcfIterator;
    private boolean isClusteringAvailable;

    public PMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MTableMetadata persistentTableMetadata, PersistentColumnFamilySortedMap pcfSortedMap)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.persistentTableMetadata = persistentTableMetadata;
        this.metadata = metadata;
        this.filter = new PCFFilter(metadata);
        this.pcfMap = pcfSortedMap;
        this.pcfIterator = this.pcfMap.entrySet().iterator();
        isClusteringAvailable = true;
    }

    public PMTableUnfilteredRowIterator(ByteBuffer key, TableMetadata metadata, MTableMetadata persistentTableMetadata, PersistentRowSingle pRowSingle)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        this.dKey = partitioner.decorateKey(key);
        this.persistentTableMetadata = persistentTableMetadata;
        this.metadata = metadata;
        this.filter = new PCFFilter(metadata);
        this.pRow = pRowSingle;
        this.pcfRowIterator = pRow.iterator();
        isClusteringAvailable = false;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public boolean isReverseOrder()
    {
        return false;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        if (metadata != null)
            return metadata.regularAndStaticColumns();
        else
            return null;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return dKey;
    }

    @Override
    public Row staticRow()
    {
        return Rows.EMPTY_STATIC_ROW;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        return DeletionTime.LIVE;
    }

    @Override
    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    @Override
    public Unfiltered computeNext()
    {
        if (isClusteringAvailable)
        {
            if (this.pcfIterator.hasNext())
            {
                return filter.getUnfilteredRowIterator(pcfIterator.next(), metadata);
            }
        }
        else
        {
            if (pcfRowIterator.hasNext())
            {
                return filter.getUnfilteredRowSingle(pcfRowIterator.next(), null, metadata);
            }
        }
        return endOfData();
    }
}
