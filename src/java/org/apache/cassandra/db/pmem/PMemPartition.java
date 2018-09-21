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
import lib.llpl.Transaction;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This struct is pointed to by the leaf nodes of the RBTree.
 *
 * The {@link #next} field deals with partitions whose keys happen to collide on the token. As that is a rarity,
 * a simple solution to handle the problem (essentially a linked list) should suffice as even if there are collisions,
 * there should be a tiny number of them ... else i guess you're screwed on that token (shrug)
 */
// TODO:JEB prolly rename me
public class PMemPartition
{
    /*
        binary format:
        - offset of partition key - 4 bytes
        // in the overwhelming majority of cases, this will be -1 (as it's rare to have a collision on token)
        - offset of partition delete info key - 4 bytes
        - address of maps of rows - 8 bytes
        - address of static row
        - address of 'next' partition - 8 bytes

        - decorated key
        -- length - 2 bytes
        -- data - length bytes
        - partition deletion data (DeletionTime.Serialier)
     */

    private static final long ROW_MAP_ADDRESS = 0; // long, address
    private static final long STATIC_ROW_ADDRESS = 8; // long, address
    private static final int DELETION_INFO_OFFSET = 16;
    private static final int DECORATED_KEY_SIZE_OFFSET = 20;
    private static final int DECORATED_KEY_OFFSET = 24;

    private static final int HEADER_SIZE = 28;

    /**
     * The block in memory where the high-level data for this partition is stored.
     */
    private final MemoryBlock block;

    private final Token token;

    // memoized DK
    private DecoratedKey key;

    // memoized map  Clustering -> Row, where Row is serialized in to one blob
    // - allows compression (debatable if needed)
    // - requires a read, deserialize, append, write semantic for updating an existing row
//    private PersistentUnsafeHashMap<ByteBuffer, ByteBuffer> rows;

    private final Heap heap;

    PMemPartition(Heap heap, MemoryBlock block, DecoratedKey dkey,Token token, PartitionUpdate update)
    {
        this.heap = heap;
        this.block = block;
        this.token= token;
        this.key = dkey;
    }

    PMemPartition(Heap heap, MemoryBlock block, DecoratedKey dkey,Token token)
    {
        this.heap = heap;
        this.block = block;
        this.token= token;
        this.key = dkey;
    }


    public static PMemPartition load(Heap heap, DecoratedKey dkey, Token token, long address)
    {
        MemoryBlock partitionBlock = heap.memoryBlockFromAddress(Raw.class, address);
        PMemPartition pMemPartition = new PMemPartition(heap, partitionBlock, dkey,token);
        return pMemPartition;
    }

    public static PMemPartition load(Heap heap,  Token token, long address)
    {
        MemoryBlock partitionBlock = heap.memoryBlockFromAddress(Raw.class, address);
        int dkSize = partitionBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
        ByteBuffer buf = ByteBuffer.allocate(dkSize);
        heap.copyToArray(partitionBlock,DECORATED_KEY_OFFSET,buf.array(),0,dkSize);
        DecoratedKey decoratedKey = new PmemDecoratedKey(token, partitionBlock, heap, buf);
        PMemPartition pMemPartition = new PMemPartition(heap, partitionBlock, decoratedKey,token);
        return pMemPartition;
    }

    public DecoratedKey getDecoratedKey()
    {
        if (key == null)
            key = new PmemDecoratedKey(token, block,heap);
        return key;
    }

    public DeletionTime getPartitionDelete() throws IOException  //TODO: Fix this later
    {
        int offset = block.getInt(DELETION_INFO_OFFSET);
        if (offset <= 0)
            return DeletionTime.LIVE;

      /*  MemoryRegionDataInputPlus inputPlus = new MemoryRegionDataInputPlus(block);
        inputPlus.position(offset);
        try
        {
            return DeletionTime.serializer.deserialize(inputPlus);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }*/
        return null;
    }

    public UnfilteredRowIterator getPmemRowMap(TableMetadata tableMetadata,ColumnFilter filter) throws IOException
    {
        long rowmapAddress = block.getLong( ROW_MAP_ADDRESS );
        if (rowmapAddress <= 0)
            return null;
        MemoryBlockDataInputPlus inputPlus = new MemoryBlockDataInputPlus(block,heap);
        //get artree from rowmap
        return PmemRowMapIterator.create(tableMetadata,inputPlus,rowmapAddress, heap, key, filter);
    }

    public UnfilteredRowIterator getPmemRowMap(TableMetadata tableMetadata,ColumnFilter filter,ClusteringIndexFilter namesFilter ) throws IOException
    {
        long rowmapAddress = block.getLong( ROW_MAP_ADDRESS );
        if (rowmapAddress <= 0)
            return null;
        MemoryBlockDataInputPlus inputPlus = new MemoryBlockDataInputPlus(block,heap);
        //get artree from rowmap
        return PmemRowMapIterator.create(tableMetadata,inputPlus,rowmapAddress, heap, key, filter, namesFilter);
    }

    public static PMemPartition create(PartitionUpdate update, Heap heap, Transaction tx) throws IOException
    {
        // handle static row seperate from regular row(s)
        final long staticRowAddress;
        if (!update.staticRow().isEmpty())
        {
            long[] staticRowAddressArray = new long[1];
            tx.execute(() -> {

                MemoryBlock staticRowRegion = heap.allocateMemoryBlock(Raw.class, staticRowAddressArray.length);
                staticRowAddressArray[0] = staticRowRegion.address();
            });
            staticRowAddress = staticRowAddressArray[0];
        }
        else
        {
            staticRowAddress = 0;
        }
        /// ROWS!!!
        final long rowMapAddress;
        if (update.hasRows())
        {
            PmemRowMap rm = PmemRowMap.create(heap, update.metadata(),tx);
            rowMapAddress = rm.getAddress(); //gets address of arTree
            for (Row r : update)
                rm.put(r, update, tx);
        }
        else
        {
            rowMapAddress = 0;
        }
        ByteBuffer key = update.partitionKey().getKey();
        PMemPartition[] partition = new PMemPartition[1];
        tx.execute(() -> {
           // int keySize = Short.BYTES + key.limit();
            int keySize =  key.limit();
            DeletionTime partitionDelete = update.deletionInfo().getPartitionDeletion();
            int partitionDeleteSize = 0;//partitionDelete.isLive() ? 0 : (int) DeletionTime.serializer.serializedSize(partitionDelete);//TODO:Fix this later
            int size = HEADER_SIZE + keySize + partitionDeleteSize;
         //   ByteBuffer buf = ByteBuffer.allocate(size);
         // TODO there's some clean up/correctness work to be done here, but the basic idea is right
            MemoryBlock block = heap.allocateMemoryBlock(Raw.class, size);
        //  MemoryBlockDataOutputPlus outputPlus = new MemoryBlockDataOutputPlus(block, 0);
            block.setLong(ROW_MAP_ADDRESS, rowMapAddress);
            block.setLong(STATIC_ROW_ADDRESS, staticRowAddress);
            if (partitionDeleteSize == 0)
            {
                block.setInt(DELETION_INFO_OFFSET, 0);
            }
            else
            {
                int offset = (int) block.address();
              /*  try
                {
                    DeletionTime.serializer.serialize(partitionDelete, outputPlus);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }*/
                block.setInt(DELETION_INFO_OFFSET, offset);
            }
           // block.setInt();
            block.setInt(DECORATED_KEY_SIZE_OFFSET, key.remaining());
            block.copyFromArray(key.array(),0,DECORATED_KEY_OFFSET , key.array().length);
            partition[0] = new PMemPartition(heap,block, update.partitionKey(),update.partitionKey().getToken(),update);

        });
        return partition[0];
    }
    // AtomicBTreePartition.addAllWithSizeDelta & RowUpdater are the places to look to see how classic storage engine stashes things

    public long getAddress()
    {
        return block.address();
    }

    public void flush()
    {
        block.flush(0, block.size());
    }

    public static PMemPartition update(PartitionUpdate update, Heap heap, long mbAddr, Transaction tx)
    {
        DecoratedKey dkey = update.partitionKey();
        Token token = dkey.getToken();
        PMemPartition[] partition = new PMemPartition[1];
        partition[0] = load(heap,dkey,token,mbAddr);
        MemoryBlock partitionBlock = heap.memoryBlockFromAddress(Raw.class,mbAddr);
        partitionBlock.addToTransaction(0,partitionBlock.size());
        long rmARTreeAddr = partitionBlock.getLong(ROW_MAP_ADDRESS);
        ByteBuffer key = update.partitionKey().getKey();
        /// ROWS!!!
        PmemRowMap rm;
        if (update.hasRows())
        {
            if(rmARTreeAddr == 0)
            {
                rm = PmemRowMap.create(heap, update.metadata(),tx);
                rmARTreeAddr = rm.getAddress(); //gets address of arTree
            }
            else
            {
                rm = PmemRowMap.loadFromAddress(heap, rmARTreeAddr, update.metadata());//PmemRowMap.create(heap, update.metadata(),new Transaction());
            }
            // build the headers data from this
            for (Row r : update)
                rm.put(r, update, tx);
        }
        MemoryBlock<Raw> mb = heap.memoryBlockFromAddress(Raw.class,mbAddr);
        final long rowmapAddress = rmARTreeAddr;
        tx.execute(() -> {
           // int keySize = Short.BYTES + key.limit();
            int keySize = key.limit();
            DeletionTime partitionDelete = update.deletionInfo().getPartitionDeletion();
            int partitionDeleteSize = 0;//
            // partitionDelete.isLive() ? 0 : (int) DeletionTime.serializer.serializedSize(partitionDelete);
            int size = HEADER_SIZE + keySize + partitionDeleteSize;
            mb.setLong(ROW_MAP_ADDRESS, rowmapAddress);
         //   mb.setLong(STATIC_ROW_ADDRESS, staticRowAddress);
            partition[0] = load(heap,update.partitionKey(),update.partitionKey().getToken(),mbAddr);//TODO: There is something odd here
        });
        return partition[0];
    }

    public static class PmemDecoratedKey extends DecoratedKey
    {
        private final MemoryBlock block;
        private final ByteBuffer key;
        private final Heap heap;

        PmemDecoratedKey(Token token, MemoryBlock memoryBlock, Heap heap) //
        {
            super(token);
            this.block = memoryBlock;
            this.heap = heap;
            this.key = getKey();

        }

        PmemDecoratedKey(Token token, MemoryBlock memoryBlock, Heap heap,ByteBuffer key)
        {
            super(token);
            this.block = memoryBlock;
            this.heap = heap;
            this.key = key;

        }

        @Override
        public ByteBuffer getKey()
        {
            int size = block.getInt(DECORATED_KEY_SIZE_OFFSET);
            ByteBuffer buf = ByteBuffer.allocate(size);
            heap.copyToArray(block,DECORATED_KEY_OFFSET,buf.array(),0,size);
            buf.limit(size);
            return buf;
        }
    }
}
