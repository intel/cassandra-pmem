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

import com.google.common.primitives.Longs;

import lib.llpl.TransactionalHeap;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

public class PmemPartitionIterator extends AbstractUnfilteredPartitionIterator
{

    private final TableMetadata tableMetadata;
    private ARTree.EntryIterator pmemPartitionIterator;
    private DecoratedKey dkey;
    private TransactionalHeap heap;
    private ColumnFilter columnFilter;
    private DataRange dataRange;

    public PmemPartitionIterator(TableMetadata tableMetadata, ARTree.EntryIterator entryIterator, TransactionalHeap heap, ColumnFilter filter, DataRange dataRange)
    {
        this.tableMetadata = tableMetadata;
        this.heap = heap;
        this.pmemPartitionIterator = entryIterator;
        this.columnFilter = filter;
        this.dataRange = dataRange;
    }

    public TableMetadata metadata()
    {
        return tableMetadata;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    public boolean hasNext()
    {
        if(pmemPartitionIterator.hasNext())
        {
            return true;
        }
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    public UnfilteredRowIterator next()
    {
        ARTree.Entry nextEntry = pmemPartitionIterator.next();
        Token token = new Murmur3Partitioner.LongToken(Longs.fromByteArray(nextEntry.getKey()));
        PmemPartition pMemPartition = PmemPartition.load(heap,token,nextEntry.getValue());
        try
        {
            if(pMemPartition != null)
                return pMemPartition.getPmemRowMap(tableMetadata, columnFilter, pMemPartition.getPartitionDelete(), dataRange);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }
}
