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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ExecutionException;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import lib.llpl.TransactionalHeap;
import lib.llpl.TransactionalMemoryBlock;
import lib.llpl.Transaction;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.pmem.linkedlist.PMemLLNode;
import org.apache.cassandra.db.pmem.linkedlist.PMemLinkedList;
import org.apache.cassandra.db.pmem.linkedlist.PMemSerializationHeaderNode;
import org.apache.cassandra.db.pmem.linkedlist.PMemTableNode;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.db.pmem.artree.ARTree;

// TODO:JEB this is really more of a dispatcher
public class TablesManager
{
    private static final Logger logger = LoggerFactory.getLogger(TablesManager.class);
    private static final int CURRENT_VERSION = 1;

    // JEB: this has to be set *once*, on first launch, as we'll use that to determine token sharding across cores,
    // in perpituity for this instance.
    private static final int CORES = FBUtilities.getAvailableProcessors();
    private static final Thread[] threads;
    private static final BlockingQueue<FutureTask<?>>[] queues;
    private static final Map<TableId, TableShard[]> tablesMap;
    static final Map<TableId, List<SerializationHeader>> tablesMetaDataMap;
    private static final TransactionalMemoryBlock rootMemoryBlock;
    private static final TransactionalMemoryBlock rawMemoryBlock;
    private static final TransactionalMemoryBlock sHeaderMemoryBlock;
    private static int rawMemoryBlockIndex = 0;
    private static final int TABLE_ID_SIZE = 36;
    private static final int TABLE_SHARD_KEY_SIZE = 24;
    private static final int TABLE_SHARD_DATAOFFSET = Integer.BYTES;//First 8 bytes contains # of table shards, offset by this while writing the ARTree addresses

    // TODO:JEB need to have a map of heaps, one entry per each dimm/namespace/etc ....
    static final TransactionalHeap heap;
    static
    {
        tablesMap = new ConcurrentHashMap<>();
        tablesMetaDataMap = new ConcurrentHashMap<>();
        String path = System.getProperty("pmem_path");
        if (path == null)
        {
            logger.error("Failed to open pool. System property \"pmem_path\" in \"conf/jvm.options\" is unset!");
            System.exit(1);
        }
        long size = 0;
        try
        {
            size = Long.parseLong(System.getProperty("pool_size"));
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to open pool. System property \"pool_size\" in \"conf/jvm.options\" is invalid!");
            System.exit(1);
        }
        heap = TransactionalHeap.getHeap(path, size);
        if (heap.getRoot() == 0)
        {
            // if there's no root set, then this is a brand new heap. thus we need to give it a new root.
            // that root is the "base address" of the map in which we'll store references to all the trees.
            rootMemoryBlock = heap.allocateMemoryBlock(16);//8 bytes for address of tablesmap, 8 for serializationheader map
            heap.setRoot(rootMemoryBlock.handle());
            rawMemoryBlock = heap.allocateMemoryBlock(12288);//64 tables for now*24 for key*8 for value which is address to tableshard[]
            rootMemoryBlock.setLong(0, rawMemoryBlock.handle());
            sHeaderMemoryBlock = heap.allocateMemoryBlock(8);//save the address of the head node for the metadata linked list
            rootMemoryBlock.setLong(8, sHeaderMemoryBlock.handle());


        }
        else
        {
            rootMemoryBlock = heap.memoryBlockFromHandle(heap.getRoot());
            long mbAddr = rootMemoryBlock.getLong(0);
            rawMemoryBlock = heap.memoryBlockFromHandle(mbAddr);
            mbAddr = rootMemoryBlock.getLong(8);
            sHeaderMemoryBlock =  heap.memoryBlockFromHandle(mbAddr);
            if(tablesMap.size() == 0)
            {
                reloadTablesMap(rawMemoryBlock);
            }
            /*if(tablesMetaDataMap.size() == 0)
            {
                reloadTablesMetadataMap(sHeaderMemoryBlock);
            }*/
        }
        // Register Allocation classes for the artree. need heap instance
        ARTree.registerAllocationClasses(heap);
        // Register row allocation class
        heap.registerAllocationSize(112, false);

        threads = new Thread[CORES];
        queues = new ArrayBlockingQueue[CORES];
        for (int i = 0; i < threads.length; i++)
        {
            final BlockingQueue queue = new ArrayBlockingQueue(1024);
            queues[i] = queue;
            threads[i] = new Thread(() ->
                {
                    try
                    {
                        execute(queue);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }, "thread-tree-" + i);
            threads[i].setDaemon(true);
            threads[i].start();
        }
    }

    private void reloadTablesMetadataMap(PMemLLNode tableNode, TableMetadata metadata)
    {
        long sHeaderValueAddr;
        long nextSHeaderBlockAddr = ((PMemTableNode)tableNode).getValue();
        tablesMetaDataMap.put(metadata.id, new ArrayList<SerializationHeader>());


        do
        {
            PMemSerializationHeaderNode headerNode = PMemSerializationHeaderNode.reload(heap, nextSHeaderBlockAddr);//heap.memoryBlockFromAddress(Raw.class, nextSHeaderBlockAddr);
            sHeaderValueAddr = headerNode.getValue();
            TransactionalMemoryBlock sHeaderValueBlock = heap.memoryBlockFromHandle(sHeaderValueAddr);
            int position= 0;

            DataInputPlus dib = new MemoryBlockDataInputPlus(sHeaderValueBlock, heap, position);
            try
            {
                SerializationHeader.Component component = SerializationHeader.serializer.deserialize(BigFormat.latestVersion, dib);
                TableMetadata.Builder tableMetadataBuilder = TableMetadata.builder(metadata.keyspace,metadata.name,metadata.id);
                component.getStaticColumns().entrySet().stream()
                      .forEach(entry -> {
                          ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                          tableMetadataBuilder.addStaticColumn(ident, entry.getValue());
                      });
                component.getRegularColumns().entrySet().stream()
                      .forEach(entry -> {
                          ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                          tableMetadataBuilder.addRegularColumn(ident, entry.getValue());
                      });
                for (ColumnMetadata columnMetadata : metadata.partitionKeyColumns())
                    tableMetadataBuilder.addPartitionKeyColumn(columnMetadata.name, columnMetadata.cellValueType());
                for (ColumnMetadata columnMetadata : metadata.clusteringColumns())
                {
                    tableMetadataBuilder.addClusteringColumn(columnMetadata.name, columnMetadata.cellValueType());
                }

                TableMetadata tableMetadata = tableMetadataBuilder.build();
                SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                                  tableMetadata,
                                                                                  tableMetadata.regularAndStaticColumns(),
                                                                                  EncodingStats.NO_STATS);
                tablesMetaDataMap.get(metadata.id).add(serializationHeader);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            nextSHeaderBlockAddr = headerNode.getNext();
        }while(nextSHeaderBlockAddr != 0);
    }

    /*
    Reconstructs the tableshard array on heap reopen
     */
    private static TableShard[] reloadTableShard(TransactionalMemoryBlock block)
    {
        //Shard count is saved first in the the memory block
        int tableShardCount = block.getInt(0);
        int position = TABLE_SHARD_DATAOFFSET;
        TableShard[] tableShards = new TableShard[tableShardCount];
        for (int i =0;i <tableShardCount;i++)
        {
            long shardAddr = block.getLong(position);
            tableShards[i] = new TableShard(heap, shardAddr);
            position +=Long.BYTES;
        }
        return tableShards;
    }

     /*
    Reconstructs the volatile tablesMap on heap reopen
     */
    private static void reloadTablesMap(TransactionalMemoryBlock block)
    {
        int position = 0;
        int len = 36; //TODO: change this hardcoded value

        byte[] b = new byte[len]; //TODO: Fix this later, get size differently
        byte[] b1 = new byte[len];

       while(position < block.size())
       {
           //Read an entry in the map from the memory block, create TableUUID from the key & a TableShard[]
           //from the value
           block.copyToArray(position, b, 0, len);
           String str = new String(b);
           if (!Arrays.equals(b, b1))
           {
               UUID tableId = UUID.fromString(str);
               position += len;
               long addr = block.getLong(position);
               position += Long.BYTES;
               TransactionalMemoryBlock tableShardBlock = heap.memoryBlockFromHandle(addr);
               TableShard[] tableShards = reloadTableShard(tableShardBlock);
               tablesMap.putIfAbsent(TableId.fromUUID(tableId), tableShards);
           }
           else break;
       }
       logger.debug("Reloaded tables\n");
    }

    public TablesManager()
    {

    }
    private static volatile boolean shutdown;
    //TODO: Can there be multiple versions with same headers, when 2 updates made simultaneously?
    public void updateMetaData(TableMetadata metadata)
    {

        SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                          metadata,
                                                                          metadata.regularAndStaticColumns(),
                                                                          EncodingStats.NO_STATS);
        long size = SerializationHeader.serializer.serializedSize(BigFormat.latestVersion,serializationHeader.toComponent());
        int position =0;
        TransactionalMemoryBlock serializationHeaderMemoryBlock = heap.allocateMemoryBlock(size);
        DataOutputPlus dob = new MemoryBlockDataOutputPlus(serializationHeaderMemoryBlock,position);
        try
        {
            SerializationHeader.serializer.serialize(BigFormat.latestVersion,serializationHeader.toComponent(),dob);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        PMemLinkedList pMemLinkedList = new PMemLinkedList(heap, sHeaderMemoryBlock.handle());
        pMemLinkedList.add(heap,metadata.id,tablesMetaDataMap.get(metadata.id).size(),serializationHeaderMemoryBlock.handle());
        if(tablesMetaDataMap.get(metadata.id) == null)
            tablesMetaDataMap.put(metadata.id, new ArrayList<SerializationHeader>());
        tablesMetaDataMap.get(metadata.id).add(serializationHeader);
    }
    private void persistSerializationHeader(ColumnFamilyStore cfs, PMemLinkedList pMemLinkedList)
    {
        SerializationHeader serializationHeader = new SerializationHeader(false,
                                                                          cfs.metadata(),
                                                                          cfs.metadata().regularAndStaticColumns(),
                                                                          EncodingStats.NO_STATS);
        long size = SerializationHeader.serializer.serializedSize(BigFormat.latestVersion, serializationHeader.toComponent());
        int position = 0;
        TransactionalMemoryBlock serializationHeaderMemoryBlock = heap.allocateMemoryBlock(size);
        DataOutputPlus dob = new MemoryBlockDataOutputPlus(serializationHeaderMemoryBlock, position);
        try
        {
            SerializationHeader.serializer.serialize(BigFormat.latestVersion, serializationHeader.toComponent(), dob);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        int headerListSize = 0;
	List<SerializationHeader> headerList = tablesMetaDataMap.get(cfs.metadata.id);
        if (headerList != null)
            headerListSize = headerList.size();
        pMemLinkedList.add(heap, cfs.metadata.id, headerListSize, serializationHeaderMemoryBlock.handle());//TODO: This makes redundant check, revisit
        tablesMetaDataMap.put(cfs.metadata.id, new ArrayList<SerializationHeader>());
        tablesMetaDataMap.get(cfs.metadata.id).add(serializationHeader);
    }

    /**
     * THIS MUST BE INVOKED IMMEDIATELY DURING/RIGHT AFTER CREATING/INSTANTIATING A CFS,
     * else everything goes to awful-ville.
     */
    public void add(TableId tableId, ColumnFamilyStore cfs)
    {
        if(!tablesMetaDataMap.containsKey(tableId))
        {
            PMemLinkedList pMemLinkedList = new PMemLinkedList(heap, sHeaderMemoryBlock.handle());
            PMemLLNode tableNode = pMemLinkedList.findTable(heap,tableId);
            if(tableNode == null)
            {
                persistSerializationHeader(cfs,pMemLinkedList);
            }
            else
            {
                TableId tableNodeId = ((PMemTableNode)tableNode).getKey();//TableId.fromString(str);
                if (tableNodeId.equals(tableId)) //It has been persisted, reload tablesmap
                {
                    reloadTablesMetadataMap(tableNode, cfs.metadata());
                }
                else   //Persist the entry
                {
                    persistSerializationHeader(cfs,pMemLinkedList);
                }
            }
        }

        if (tablesMap.containsKey(tableId))
            return;



        // All system tables are assigned to shard 0 currently
        // TODO: Should system tables be distributed like user keyspaces? Revisit
        int treeCount ;
        final TransactionalMemoryBlock rawTablesMemoryBlock;
        if((SchemaConstants.isLocalSystemKeyspace(cfs.keyspace.getName())) || (SchemaConstants.isReplicatedSystemKeyspace(cfs.keyspace.getName())))
        {
            rawTablesMemoryBlock = heap.allocateMemoryBlock(Long.BYTES + Integer.BYTES );//save the # of entries and the memoryblock address for the shards
            treeCount = 1;
        }
        else
        {
            rawTablesMemoryBlock = heap.allocateMemoryBlock((Long.BYTES * CORES) + Integer.BYTES);
            treeCount = CORES;
        }
        TableShard[] tableShards = new TableShard[treeCount];
        Transaction.run(heap,() ->
            {
                rawTablesMemoryBlock.setInt(0, treeCount );
                for (int i = 0; i < treeCount; i++)
                {
                    //Save address of all the shards for the table
                    TableShard tableShard = new TableShard(heap);
                    rawTablesMemoryBlock.setLong((i*Long.BYTES) + TABLE_SHARD_DATAOFFSET, tableShard.getAddress());
                    tableShards[i] = tableShard;
                }
                if (tablesMap.putIfAbsent(tableId, tableShards) == null)
                {
                    byte[] tableUUIDBytes = tableId.toString().getBytes();
                    rawMemoryBlock.copyFromArray(tableUUIDBytes, 0, rawMemoryBlockIndex, tableUUIDBytes.length);
                    rawMemoryBlockIndex += TABLE_ID_SIZE;
                    rawMemoryBlock.setLong(rawMemoryBlockIndex, rawTablesMemoryBlock.handle());
                    rawMemoryBlockIndex += Long.BYTES;
                }
            });
    }



    private static String toKey(TableId tableId, int shard)
    {
        return String.format("%s-%d-%d", tableId.toString(), shard, CURRENT_VERSION);
    }

    /*
        public functions to operate on data
    */
    public Future<Void> apply(ColumnFamilyStore cfs, final PartitionUpdate update, final UpdateTransaction indexer, final OpOrder.Group opGroup)
    {
        if (shutdown)
            return Futures.immediateCancelledFuture();
        ;
        final int index;
        if((SchemaConstants.isLocalSystemKeyspace(cfs.keyspace.getName())) || (SchemaConstants.isReplicatedSystemKeyspace(cfs.keyspace.getName())))
        {
            index = 0 ;
        }
        else
        {
            index = Math.abs(findTreeIndex(update.partitionKey()));
        }
        final FutureTask<Void> future = new FutureTask<>(new WriteOperation(getTree(cfs.metadata.id, index), update, indexer, opGroup));
        try
        {
            queues[index].put(future);
        }
        catch (InterruptedException e)
        {
            logger.info("couldn't add query to queue");
            e.printStackTrace();
        }
        return future;
    }

    private int findTreeIndex(DecoratedKey key)
    {
        // JEB: this is weak ... but good enough for now. don't want to build token range checking atm ....
        Long l = (Long)key.getToken().getTokenValue();
        return Math.abs((int)(l % CORES));
    }

    private TableShard getTree(TableId tableId, int index)
    {
        TableShard[] trees = tablesMap.get(tableId);
        if (trees != null)
        {
            return trees[index];
        }
        throw new IllegalStateException("attempted to get a tree for a CFS that has not been initialized yet; table Id = " + tableId);
    }



    public Future<UnfilteredRowIterator> select(ColumnFamilyStore cfs, ColumnFilter filter, DecoratedKey decoratedKey) //TODO: do we need 2 of these?
    {
        if (shutdown)
            return Futures.immediateCancelledFuture();
        int index;
        if((SchemaConstants.isLocalSystemKeyspace(cfs.keyspace.getName())) || (SchemaConstants.isReplicatedSystemKeyspace(cfs.keyspace.getName())))
        {
            index = 0 ;
        }
        else
        {
            index = findTreeIndex(decoratedKey);
        }
        final FutureTask<UnfilteredRowIterator> future = new FutureTask<>(new ReadOperation(getTree(cfs.metadata.id, index), cfs.metadata(), filter,decoratedKey));
        try
        {
            queues[index].put(future);
        }
        catch (InterruptedException e)
        {
            logger.info("couldn't add query to queue");
            e.printStackTrace();
        }
        return future;
    }

    public void garbageCollect(ColumnFamilyStore cfs)
    {
        FutureTask<Void>[] futures = new FutureTask[queues.length];
        for(int i=0; i<queues.length; i++) {
            FutureTask<Void> future = new FutureTask<>(new VacuumOperation(getTree(cfs.metadata.id, i)));
            futures[i] = future;
            try {
                queues[i].put(future);
            } catch (InterruptedException e) {
                logger.info("Error occured during vacuum");
                e.printStackTrace();
            }
        }

        for (int i=0; i<futures.length; i++) {
            while(!futures[i].isDone()){}
        }
    }

    private Future<UnfilteredRowIterator> select(ColumnFamilyStore cfs, ColumnFilter filter, ClusteringIndexFilter namesFilter, DecoratedKey decoratedKey)
    {
        if (shutdown)
            return Futures.immediateCancelledFuture();
        int index;
        if((SchemaConstants.isLocalSystemKeyspace(cfs.keyspace.getName())) || (SchemaConstants.isReplicatedSystemKeyspace(cfs.keyspace.getName())))
        {
            index = 0 ;
        }
        else
        {
            index = findTreeIndex(decoratedKey);
        }
        final FutureTask<UnfilteredRowIterator> future = new FutureTask<>(new ReadOperation(getTree(cfs.metadata.id, index), cfs.metadata(), filter, namesFilter,decoratedKey));
        try
        {
            queues[index].put(future);
        }
        catch (InterruptedException e)
        {
            logger.info("couldn't add query to queue");
            e.printStackTrace();
        }
        return future;
    }

    public UnfilteredPartitionIterator makePartitionIterator(ColumnFamilyStore cfs, PartitionRangeReadCommand readCommand)
    {
        DataRange dataRange = readCommand.dataRange();
        TableMetadata metadata = readCommand.metadata();
        ColumnFilter columnFilter = readCommand.columnFilter();
        TableShard[] table = tablesMap.get(cfs.metadata.id);
        final List<UnfilteredPartitionIterator> iterators = new ArrayList<>(table.length);
        for(int i=0;i<table.length;i++)
        {
            try
            {
                UnfilteredPartitionIterator iter =  table[i].getPmemPartitionMap(metadata, columnFilter, dataRange);
                if(iter != null)
                    iterators.add(iter);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return iterators.isEmpty() ? EmptyIterators.unfilteredPartition(metadata)
                                   : UnfilteredPartitionIterators.mergeLazily(iterators, FBUtilities.nowInSeconds());
    }

    public Future<UnfilteredRowIterator> makeRowIterator(ColumnFamilyStore cfs, ColumnFilter filter, DecoratedKey partitionKey)
    {
        return(select( cfs,filter, partitionKey));
    }

    public Future<UnfilteredRowIterator> makeRowIterator(ColumnFamilyStore cfs, ColumnFilter filter, ClusteringIndexFilter indexFilter, DecoratedKey partitionKey)
    {
        return(select( cfs,filter, indexFilter,partitionKey));
    }

    /*
        async tree functions
     */
    private void handleWrite(TableShard tableShard, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        // basically ignore the indexer and opGroup ... for now
        // TODO: probably need a try/catch block here ...
        tableShard.apply(update,indexer,opGroup);
    }

    private UnfilteredRowIterator handleRead(TableShard tableShard, ColumnFilter filter, DecoratedKey decoratedKey, TableMetadata tableMetadata) //throws IOException
    {
        // TODO: wrap the response in a UnfilteredRowIterator, somehows...
       return tableShard.get(decoratedKey, filter, tableMetadata);
    }

    private UnfilteredRowIterator handleRead(TableShard tableShard, ColumnFilter filter, ClusteringIndexFilter namesFilter,DecoratedKey decoratedKey, TableMetadata tableMetadata) //throws IOException
    {
        // TODO: wrap the response in a UnfilteredRowIterator, somehows...
        return tableShard.get(decoratedKey, filter,namesFilter,tableMetadata);
    }

    private void vacuum(TableShard tableShard) {
        tableShard.vacuum();
    }

    /*
        thread / queue functions
     */
    public static void execute(BlockingQueue<FutureTask> queue) throws InterruptedException
    {
        while(true)
        {
            try
            {
                FutureTask entry = queue.take();
                entry.run();
                if(entry.get() instanceof PmemRowMapIterator) ((PmemRowMapIterator)(entry.get())).ack.get();
            }
            catch(InterruptedException e)
            {
                e.printStackTrace();
            }
            catch(ExecutionException e)
            {
                e.printStackTrace();
            }
        }

    }

    class WriteOperation implements Callable<Void>
    {
        private final TableShard tableShard;
        private final PartitionUpdate update;
        private final UpdateTransaction indexer;
        private final OpOrder.Group opGroup;

        WriteOperation(TableShard tableShard, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            this.tableShard = tableShard;
            this.update = update;
            this.indexer = indexer;
            this.opGroup = opGroup;
        }

        @Override
        public Void call()
        {
            handleWrite(tableShard, update, indexer, opGroup);
            return null;
        }
    }

    class VacuumOperation implements Callable<Void>
    {
        private final TableShard tableShard;

        VacuumOperation(TableShard tableShard)
        {
            this.tableShard = tableShard;
        }

        @Override
        public Void call()
        {
            vacuum(tableShard);
            return null;
        }
    }

    class ReadOperation implements Callable<UnfilteredRowIterator>
    {
        private final TableShard tableShard;
        private final DecoratedKey decoratedKey;
        private final ColumnFilter filter;
        private ClusteringIndexFilter namesFilter = null;
        private final TableMetadata tableMetadata;

        ReadOperation(TableShard tableShard, TableMetadata metadata, ColumnFilter filter, DecoratedKey decoratedKey)
        {
            this.tableShard = tableShard;
            this.decoratedKey = decoratedKey;
            this.filter = filter;
            this.tableMetadata = metadata;
        }

        ReadOperation(TableShard tableShard, TableMetadata metadata, ColumnFilter filter, ClusteringIndexFilter namesFilter,DecoratedKey decoratedKey)
        {
            this.tableShard = tableShard;
            this.decoratedKey = decoratedKey;
            this.filter = filter;
            this.namesFilter = namesFilter;
            this.tableMetadata = metadata;
        }

        @Override
        public UnfilteredRowIterator call() //throws IOException
        {
            if (namesFilter != null) // TODO: Revisit later
            {
                return handleRead(tableShard, filter, namesFilter, decoratedKey, tableMetadata);
            }
            return  handleRead(tableShard, filter, decoratedKey,tableMetadata);
        }
    }

    public void shutdown()
    {
        shutdown = true;
    }
}
