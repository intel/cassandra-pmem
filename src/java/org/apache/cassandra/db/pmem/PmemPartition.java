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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lib.llpl.TransactionalHeap;
import lib.llpl.TransactionalMemoryBlock;
import lib.llpl.Transaction;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
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
public class PmemPartition
{
    private static final Logger logger = LoggerFactory.getLogger(PmemPartition.class);
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

    private static final long ROW_MAP_ADDRESS = 0; // long, address //TODO: resuse this for deletion info
    private static final long STATIC_ROW_ADDRESS = 8; // long, address
    private static final byte DELETION_INFO_OFFSET = 16;  //TODO: remove this
    private static final long NEXT_PARTITION_ADDRESS = 17; //TODO: move this up
    private static final int DECORATED_KEY_SIZE_OFFSET = 25;
    private static final int DECORATED_KEY_OFFSET = 29;

    private static final int HEADER_SIZE = 33;

    /**
     * The block in memory where the high-level data for this partition is stored.
     */
    private final TransactionalMemoryBlock block;

    private final Token token;

    // memoized DK
    private DecoratedKey key;

    // memoized map  Clustering -> Row, where Row is serialized in to one blob
    // - allows compression (debatable if needed)
    // - requires a read, deserialize, append, write semantic for updating an existing row
//    private PersistentUnsafeHashMap<ByteBuffer, ByteBuffer> rows;

    private final TransactionalHeap heap;
    private final DeletionTime deletionTime;

    PmemPartition(TransactionalHeap heap, TransactionalMemoryBlock block, DecoratedKey dkey,Token token, DeletionTime deletionTime)
    {
        this.heap = heap;
        this.block = block;
        this.token= token;
        this.key = dkey;
        this.deletionTime = deletionTime;
    }

    PmemPartition(TransactionalHeap heap, TransactionalMemoryBlock block, DecoratedKey dkey,Token token)
    {
        this(heap, block, dkey, token, DeletionTime.LIVE);
    }

    static boolean isTombstone(TransactionalMemoryBlock block) {
        return block.getByte(DELETION_INFO_OFFSET) == (byte)-1;
    }

    public static PmemPartition load(TransactionalHeap heap, DecoratedKey dkey, Token token, long address)
    {
        TransactionalMemoryBlock partitionBlock = heap.memoryBlockFromHandle(address);
        //read DK from MemoryBlock & check for collision
        int dkSize;
        dkSize = partitionBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
        ByteBuffer buf = ByteBuffer.allocate(dkSize);
        partitionBlock.copyToArray(DECORATED_KEY_OFFSET,buf.array(),0,dkSize);
        DecoratedKey storedDecoratedKey;
        storedDecoratedKey = new PmemDecoratedKey(token, partitionBlock, heap, buf);
        PmemPartition pMemPartition;
        if(dkey.equals(storedDecoratedKey))
        {
            pMemPartition = new PmemPartition(heap, partitionBlock, dkey,token);
            return pMemPartition;
        }
        else
        {
            logger.info("Collision detected during reads!!!");
            TransactionalMemoryBlock nextBlock = partitionBlock;
            while(nextBlock.getLong(NEXT_PARTITION_ADDRESS)!= 0)
            {
                nextBlock =  heap.memoryBlockFromHandle(nextBlock.getLong(NEXT_PARTITION_ADDRESS));
                dkSize = nextBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
                ByteBuffer buf1 = ByteBuffer.allocate(dkSize);
                nextBlock.copyToArray(DECORATED_KEY_OFFSET,buf1.array(),0,dkSize);
                storedDecoratedKey = new PmemDecoratedKey(token, nextBlock, heap, buf1);
                if(dkey.equals(storedDecoratedKey))
                {
                    logger.info("Collision detected during reads!!!");
                    pMemPartition = new PmemPartition(heap, nextBlock, dkey,token);
                    return pMemPartition;
                }
            }
        }
        return null;
       // pMemPartition = new PMemPartition(heap, partitionBlock, dkey,token);
       // return pMemPartition;
    }

    public static PmemPartition load(TransactionalHeap heap,  Token token, long address)
    {
        TransactionalMemoryBlock partitionBlock = heap.memoryBlockFromHandle(address);
        int dkSize = partitionBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
        ByteBuffer buf = ByteBuffer.allocate(dkSize);
        partitionBlock.copyToArray(DECORATED_KEY_OFFSET,buf.array(),0,dkSize);
        DecoratedKey decoratedKey = new PmemDecoratedKey(token, partitionBlock, heap, buf);
        PmemPartition pMemPartition = new PmemPartition(heap, partitionBlock, decoratedKey,token);
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
        byte isPartitionDeleted = block.getByte(DELETION_INFO_OFFSET);
        if (isPartitionDeleted >= 0)
            return DeletionTime.LIVE;
        TransactionalMemoryBlock partitonBlock = heap.memoryBlockFromHandle(getAddress());
        MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(partitonBlock, heap);
        try
        {
            return DeletionTime.serializer.deserialize(memoryBlockDataInputPlus);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    public UnfilteredRowIterator getPmemRowMap(TableMetadata tableMetadata, ColumnFilter filter, DeletionTime partitionDelete) throws IOException
    {
        if(block.getByte(DELETION_INFO_OFFSET) == -1)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);
        long rowmapAddress = block.getLong( ROW_MAP_ADDRESS );
        if (rowmapAddress <= 0)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);
        MemoryBlockDataInputPlus inputPlus = new MemoryBlockDataInputPlus(block,heap);
        //get artree from rowmap
        return PmemRowMapIterator.create(tableMetadata,inputPlus,rowmapAddress, heap, key, filter,partitionDelete);
    }

    public UnfilteredRowIterator getPmemRowMap(TableMetadata tableMetadata,ColumnFilter filter,ClusteringIndexFilter namesFilter, DeletionTime partitionDelete ) throws IOException
    {
        if(block.getByte(DELETION_INFO_OFFSET) == -1)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);
        long rowmapAddress = block.getLong( ROW_MAP_ADDRESS );
        if (rowmapAddress <= 0)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);;
        MemoryBlockDataInputPlus inputPlus = new MemoryBlockDataInputPlus(block,heap);
        //get artree from rowmap
        return PmemRowMapIterator.create(tableMetadata,inputPlus,rowmapAddress, heap, key, filter, namesFilter, partitionDelete);
    }

    public UnfilteredRowIterator getPmemRowMap(TableMetadata tableMetadata,ColumnFilter filter, DeletionTime partitionDelete, DataRange dataRange ) throws IOException
    {
        if(block.getByte(DELETION_INFO_OFFSET) == -1)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);
        long rowmapAddress = block.getLong( ROW_MAP_ADDRESS );
        if (rowmapAddress <= 0)
            return EmptyIterators.unfilteredRow(tableMetadata, key, false);;
        MemoryBlockDataInputPlus inputPlus = new MemoryBlockDataInputPlus(block,heap);
        //get artree from rowmap
        return PmemRowMapIterator.create(tableMetadata,inputPlus,rowmapAddress, heap, key, filter,  partitionDelete, dataRange);
    }

    public static PmemPartition create(PartitionUpdate update, TransactionalHeap heap, Transaction tx) throws IOException
    {
        // handle static row seperate from regular row(s)
        final long staticRowAddress;
        if (!update.staticRow().isEmpty())
        {
            long[] staticRowAddressArray = new long[1];
            tx.run(() -> {

                TransactionalMemoryBlock staticRowRegion = heap.allocateMemoryBlock(staticRowAddressArray.length);
                staticRowAddressArray[0] = staticRowRegion.handle();
            });
            staticRowAddress = staticRowAddressArray[0];
        }
        else
        {
            staticRowAddress = 0;
        }
        /// ROWS!!!
        final long rowMapAddress;

        PmemRowMap rm = PmemRowMap.create(heap, update.metadata(),update.partitionLevelDeletion(),tx);
        rowMapAddress = rm.getAddress(); //gets address of arTree
        for (Row r : update)
            rm.put(r, update, tx);

        DeletionTime partitionDeleteTime = update.deletionInfo().getPartitionDeletion();
        ByteBuffer key = update.partitionKey().getKey();
        PmemPartition[] partition = new PmemPartition[1];
        tx.run(() -> {
            int keySize =  key.limit();
            int partitionDeleteSize = partitionDeleteTime.isLive() ? 0 : (int) DeletionTime.serializer.serializedSize(partitionDeleteTime);//TODO:Fix this later
            int size = HEADER_SIZE + keySize;
            // TODO there's some clean up/correctness work to be done here, but the basic idea is right
            TransactionalMemoryBlock block = heap.allocateMemoryBlock(size);

            if (partitionDeleteSize != 0)
            {
                MemoryBlockDataOutputPlus partitionBlockOutputPlus = new MemoryBlockDataOutputPlus(block, 0);
                try
                {
                    DeletionTime.serializer.serialize(partitionDeleteTime,partitionBlockOutputPlus);
                    block.setByte(DELETION_INFO_OFFSET, (byte) -1); //This indicates it is a Tombstone
                    block.setInt(DECORATED_KEY_SIZE_OFFSET, key.remaining());
                    block.copyFromArray(key.array(),0,DECORATED_KEY_OFFSET , key.array().length);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                block.setLong(ROW_MAP_ADDRESS, rowMapAddress);
                block.setLong(STATIC_ROW_ADDRESS, staticRowAddress);
                block.setInt(DECORATED_KEY_SIZE_OFFSET, key.remaining());
                block.copyFromArray(key.array(),0,DECORATED_KEY_OFFSET , key.array().length);
            }
            partition[0] = new PmemPartition(heap, block, update.partitionKey(), update.partitionKey().getToken(), update.partitionLevelDeletion());//, update.deletionInfo().getPartitionDeletion());

        });
        return partition[0];
    }
    // AtomicBTreePartition.addAllWithSizeDelta & RowUpdater are the places to look to see how classic storage engine stashes things

    public long getAddress()
    {
        return block.handle();
    }

    public void flush()
    {
        //block.flush(0, block.size());
    }

    public static PmemPartition update(PartitionUpdate update, TransactionalHeap heap, long mbAddr, Transaction tx)
    {
        PmemPartition[] partition = new PmemPartition[1];

        DecoratedKey dkey = update.partitionKey();
        Token token = dkey.getToken();
        //create MemoryBlock from address, read DK from it & check for conflicts
        TransactionalMemoryBlock partitionBlock = heap.memoryBlockFromHandle(mbAddr);
        int dkSize = partitionBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
        ByteBuffer buf = ByteBuffer.allocate(dkSize);
        partitionBlock.copyToArray(DECORATED_KEY_OFFSET,buf.array(),0,dkSize);
        DecoratedKey storedDecoratedKey = new PmemDecoratedKey(token, partitionBlock, heap, buf);

        if(dkey.equals(storedDecoratedKey))
        {
            return updatePMemPartition(heap, partitionBlock, dkey, update,tx);
        }
        else
        {
            logger.info("Collision detected during writes!!!");
            try
            {
                TransactionalMemoryBlock nextBlock = partitionBlock;
                while(nextBlock.getLong(NEXT_PARTITION_ADDRESS)!= 0) //TODO: This needs some real changes!!!

                {
                    nextBlock =  heap.memoryBlockFromHandle(nextBlock.getLong(NEXT_PARTITION_ADDRESS));
                    dkSize = nextBlock.getInt(DECORATED_KEY_SIZE_OFFSET);
                    ByteBuffer buf1 = ByteBuffer.allocate(dkSize);
                    nextBlock.copyToArray(DECORATED_KEY_OFFSET,buf1.array(),0,dkSize);
                    storedDecoratedKey = new PmemDecoratedKey(token, nextBlock, heap, buf1);
                    if(dkey.equals(storedDecoratedKey))
                    {
                        return updatePMemPartition(heap, nextBlock, dkey, update,tx);
                    }
                }

                final TransactionalMemoryBlock nextBlk = nextBlock;
                tx.run(() ->
                   {
                       PmemPartition nextPartition = null;
                       try
                       {
                           nextPartition = PmemPartition.create(update, heap, tx);
                       }
                       catch (IOException e)
                       {
                           e.printStackTrace();
                       }
                       //nextBlk.addToTransaction(0, nextBlk.size()); //TODO: check if this is correct
                       nextBlk.setLong(NEXT_PARTITION_ADDRESS, nextPartition.getAddress());
                   });
                partition[0] = new PmemPartition(heap, nextBlk, dkey, token, update.partitionLevelDeletion());//, deletionTime);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        return partition[0];
    }

    //3 different scenarios will be addressed :
    // 1) updating a partition
    // 2) Deleting a partition
    // 3) updating a deleted partition
    private static PmemPartition updatePMemPartition(TransactionalHeap heap, TransactionalMemoryBlock partitionBlock, DecoratedKey dkey, PartitionUpdate update,Transaction tx)
    {
        Token token = dkey.getToken();
        DeletionTime partitionDeleteTime = update.deletionInfo().getPartitionDeletion();
        PmemRowMap pmemRowMap = null;
        //partitionBlock.addToTransaction(0, partitionBlock.size());
        if(partitionBlock.getByte(DELETION_INFO_OFFSET) != -1)
        {
            long rmARTreeAddr = partitionBlock.getLong(ROW_MAP_ADDRESS);
            pmemRowMap = PmemRowMap.loadFromAddress(heap, rmARTreeAddr, update.metadata(), partitionDeleteTime);//PmemRowMap.create(heap, update.metadata(),new Transaction());
        }
        else if (update.hasRows()) //update after delete ??
        {
            final long staticRowAddress;
            if (!update.staticRow().isEmpty())
            {
                long[] staticRowAddressArray = new long[1];
                tx.run(() -> {

                    TransactionalMemoryBlock staticRowRegion = heap.allocateMemoryBlock(staticRowAddressArray.length);
                    staticRowAddressArray[0] = staticRowRegion.handle();
                });
                staticRowAddress = staticRowAddressArray[0];
            }
            else
            {
                staticRowAddress = 0;
            }
            pmemRowMap = PmemRowMap.create(heap, update.metadata(), update.partitionLevelDeletion(), tx);
            partitionBlock.setByte(DELETION_INFO_OFFSET, (byte) 0);
            partitionBlock.setLong(ROW_MAP_ADDRESS, pmemRowMap.getAddress());
            partitionBlock.setLong(STATIC_ROW_ADDRESS, staticRowAddress);
        }
        /// ROWS!!!
        //TODO: Do we need to merge deletion info?
        // build the headers data from this
        for (Row r : update)
            pmemRowMap.put(r, update, tx);
        final PmemRowMap rm = pmemRowMap;
        tx.run(() ->
           {
               int partitionDeleteSize = partitionDeleteTime.isLive() ? 0 : (int) DeletionTime.serializer.serializedSize(partitionDeleteTime);
               if(partitionDeleteSize > 0) //need to check if partitionDelete exists if so free it, can we inline it!!
               {
                   MemoryBlockDataOutputPlus partitionBlockOutputPlus = new MemoryBlockDataOutputPlus(partitionBlock, 0);
                   try
                   {
                       if(rm != null)
                       {
                           rm.delete();
                           DeletionTime.serializer.serialize(partitionDeleteTime, partitionBlockOutputPlus);
                           partitionBlock.setByte(DELETION_INFO_OFFSET, (byte) -1); //This indicates it is a Tombstone
                       }
                   }
                   catch (IOException e)
                   {
                       e.printStackTrace();
                   }
               }
           });
        PmemPartition pMemPartition = new PmemPartition(heap, partitionBlock, dkey, token, update.partitionLevelDeletion());//, deletionTime);
        return pMemPartition;

    }

    public static class PmemDecoratedKey extends DecoratedKey
    {
        private final TransactionalMemoryBlock block;
        private final ByteBuffer key;
        private final TransactionalHeap heap;

        PmemDecoratedKey(Token token, TransactionalMemoryBlock memoryBlock, TransactionalHeap heap) //
        {
            super(token);
            this.block = memoryBlock;
            this.heap = heap;
            this.key = getKey();

        }

        PmemDecoratedKey(Token token, TransactionalMemoryBlock memoryBlock, TransactionalHeap heap,ByteBuffer key)
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
            block.copyToArray(DECORATED_KEY_OFFSET,buf.array(),0,size);
            buf.limit(size);
            return buf;
        }
    }
}
