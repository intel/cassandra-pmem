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
import lib.llpl.Heap;
import lib.llpl.Transaction;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.schema.TableMetadata;

// key type = DectoratedKey
// value type = PMemParition
public class TableShard
{
    private static final int MAX_INTERNAL_KEYS = 8;
    private static final int MAX_LEAF_KEYS = 64;

    /**
     * HEAD_LEAF is a fixed sentinal for this tree instance. It is critical for restarting the process, as it allows us
     * to have a known starting address from which to reconstruct the tree.
     */
    //   private final PersistentLeaf HEAD_LEAF;
    private static Heap heap;
    private final ARTree arTree;

    public TableShard(Heap heap)
    {
        this.heap = heap;
        this.arTree = new ARTree(heap);
    }

    // hmm, I guess this ctor is the "reconstruct entire tree" entry point, as we have the addr for
    // the first persistent leaf ...
   // public TableShard(Heap heap, long address, TableMetadata tableMetadata)
    public TableShard(Heap heap, long address)
    {
        this.heap = heap;
        this.arTree = new ARTree(heap, address);
    }

    public void apply(PartitionUpdate update)
    {
        DecoratedKey decoratedKey = update.partitionKey();
        long token = (Long) decoratedKey.getToken().getTokenValue();
        Transaction tx = new Transaction(heap);
        tx.execute(()->
           {
                insert(token, update, tx);
           });
    }

    static Long merge(Object update, Long mb)
    {
        Transaction tx = new Transaction(heap);
        return tx.execute(() ->
          {
              if (mb == 0)
              {
                  try
                  {
                      PMemPartition pMemPartition = PMemPartition.create( (PartitionUpdate)update, heap, tx);
                      pMemPartition.flush();
                      return (pMemPartition.getAddress());
                  }
                  catch (Exception e)
                  {
                      e.printStackTrace();
                  }
              }
              PMemPartition pMemPartition = PMemPartition.update((PartitionUpdate) update, heap, mb, tx);
              return (pMemPartition.getAddress());
          });
    }

    public void insert( long token, PartitionUpdate update, Transaction tx)
    {
        if (update.hasRows())
        {
            arTree.apply(Longs.toByteArray(token), update, TableShard::merge, tx);
        }
    }

    public UnfilteredRowIterator get(DecoratedKey key, ColumnFilter filter, TableMetadata metadata) //throws IOException
    {
        if(key != null)
        {
            long token = (Long) key.getToken().getTokenValue();
            byte[] tokenBytes = Longs.toByteArray(token);
            long partionAddr = arTree.get(tokenBytes);
            if (partionAddr == 0)
            {
                return EmptyIterators.unfilteredRow(metadata, key, false);
            }
            PMemPartition pMemPartition = PMemPartition.load(heap, key, new LongToken(token), partionAddr);
            try
            {
                return pMemPartition.getPmemRowMap(metadata, filter);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return null; //TODO: return empty iterator?
    }

    public UnfilteredPartitionIterator getPmemPartitionMap(TableMetadata metadata, ColumnFilter filter, DataRange dataRange) throws IOException
    {
        long offset = arTree.address();
        if (offset <= 0) return null;
        PMemPartitionIterator pMemPartitionIterator = new PMemPartitionIterator(metadata,arTree.getEntryIterator(), heap, filter, dataRange);
        //get artree from rowmap
        return pMemPartitionIterator;
    }

    public long getAddress()
    {
        return arTree.address();
    }

    public UnfilteredRowIterator get(DecoratedKey decoratedKey, ColumnFilter filter, ClusteringIndexFilter namesFilter, TableMetadata metadata)
    {
        if(decoratedKey != null)
        {
            long token = (Long) decoratedKey.getToken().getTokenValue();
            byte[] tokenBytes = Longs.toByteArray(token);
            long partionAddr = arTree.get(tokenBytes);
            if (partionAddr == 0)
            {
                return EmptyIterators.unfilteredRow(metadata, decoratedKey, false);
            }
            PMemPartition pMemPartition = PMemPartition.load(heap, decoratedKey, new LongToken(token), partionAddr);
            try
            {
                return pMemPartition.getPmemRowMap(metadata, filter, namesFilter);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }
}


