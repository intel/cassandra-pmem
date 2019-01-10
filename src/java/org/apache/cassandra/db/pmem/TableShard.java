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
import java.util.function.BiFunction;
import com.google.common.primitives.Longs;
import lib.llpl.TransactionalHeap;
import lib.llpl.*;
import lib.llpl.Transaction;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.db.pmem.artree.ARTree;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

// key type = DectoratedKey
// value type = PMemParition
public class TableShard
{
    /**
     * arTree is a fixed sentinal for this tree instance. It is critical for restarting the process, as it allows us
     * to have a known starting address from which to reconstruct the tree.
     */
    private static TransactionalHeap heap;
    private final ARTree arTree;

    public TableShard(TransactionalHeap heap)
    {
        this.heap = heap;
        this.arTree = new ARTree(heap);
    }

    // hmm, I guess this ctor is the "reconstruct entire tree" entry point, as we have the addr for
    // the first persistent leaf ...
   // public TableShard(Heap heap, long address, TableMetadata tableMetadata)
    public TableShard(TransactionalHeap heap, long address)
    {
        this.heap = heap;
        this.arTree = new ARTree(heap, address);
    }

    public void apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        DecoratedKey decoratedKey = update.partitionKey();
        long token = (Long) decoratedKey.getToken().getTokenValue();
        Transaction tx = new Transaction(heap);
        tx.run(()->
           {
               insert(token, update, indexer, opGroup, tx);
           });
    }

    public void vacuum() {
        BiFunction <byte[], Long, ARTree.Operation> fcn = (k, v)->{
            TransactionalMemoryBlock mb = heap.memoryBlockFromHandle(v);
            if (PmemPartition.isTombstone(mb)) {
                mb.free();
                return ARTree.Operation.DELETE_NODE;
            }
                return ARTree.Operation.NO_OP;
        };
        arTree.forEach(fcn);
    }

    public void insert(long token, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, Transaction tx)
    {
        arTree.apply(Longs.toByteArray(token), update, TableShard::merge, tx);
    }

    static Long merge(Object update, Long mb)
    {
        Transaction tx = new Transaction(heap);
        return tx.run(() ->
          {
              if (mb == 0)
              {
                  try
                  {
                      PmemPartition pMemPartition = PmemPartition.create( (PartitionUpdate)update, heap, tx);
                      return (pMemPartition.getAddress());
                  }
                  catch (Exception e)
                  {
                      e.printStackTrace();
                  }
              }
              PmemPartition pMemPartition = PmemPartition.update((PartitionUpdate) update, heap, mb, tx);
              return (pMemPartition.getAddress());
          });
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
            PmemPartition pMemPartition = PmemPartition.load(heap, key, new LongToken(token), partionAddr);
            try
            {
                if(pMemPartition != null)
                {
                    return pMemPartition.getPmemRowMap(metadata, filter, pMemPartition.getPartitionDelete());
                }
                else
                {
                    return EmptyIterators.unfilteredRow(metadata, key, false);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return EmptyIterators.unfilteredRow(metadata, key, false); //TODO: return empty iterator?
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
            PmemPartition pMemPartition = PmemPartition.load(heap, decoratedKey, new LongToken(token), partionAddr);
            try
            {
                if(pMemPartition != null)
                {
                    return pMemPartition.getPmemRowMap(metadata, filter, namesFilter, pMemPartition.getPartitionDelete());
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
       return EmptyIterators.unfilteredRow(metadata, decoratedKey, false);
    }

    public UnfilteredPartitionIterator getPmemPartitionMap(TableMetadata metadata, ColumnFilter filter, DataRange dataRange) throws IOException
    {
        long offset = getAddress();
        if (offset <= 0) return null;

        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        PmemPartitionIterator pMemPartitionIterator;
        Token.TokenFactory tokenFactory = metadata.partitioner.getTokenFactory();
        if (startIsMin)
        {
            if(stopIsMin)
            {
                pMemPartitionIterator = new PmemPartitionIterator(metadata,arTree.getEntryIterator(), heap, filter, dataRange);
            }
            else
            {
                byte[] rightTokenBytes = tokenFactory.toByteArray(keyRange.right.getToken()).array();
                pMemPartitionIterator = new PmemPartitionIterator(metadata,arTree.getEntryIterator(rightTokenBytes,includeStop), heap, filter, dataRange);
            }
        }
        else
        {
            if(stopIsMin)
            {
                byte[] leftTokenBytes = tokenFactory.toByteArray(keyRange.left.getToken()).array();
                pMemPartitionIterator = new PmemPartitionIterator(metadata,arTree.getEntryIterator(leftTokenBytes, includeStart), heap, filter, dataRange);
            }
            else
            {
                byte[]  leftTokenBytes = tokenFactory.toByteArray(keyRange.left.getToken()).array();
                byte[] rightTokenBytes = tokenFactory.toByteArray(keyRange.right.getToken()).array();
                pMemPartitionIterator = new PmemPartitionIterator(metadata,arTree.getEntryIterator(leftTokenBytes,includeStart,rightTokenBytes,includeStop), heap, filter, dataRange);
            }

        }
        return pMemPartitionIterator;
    }

    public long getAddress()
    {
        return arTree.address();
    }
}


