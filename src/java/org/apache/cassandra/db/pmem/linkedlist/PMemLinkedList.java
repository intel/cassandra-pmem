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

package org.apache.cassandra.db.pmem.linkedlist;


import lib.llpl.TransactionalHeap;
import lib.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.schema.TableId;

public class PMemLinkedList
{
    TransactionalMemoryBlock head;

    public PMemLinkedList(TransactionalHeap heap,long address)
    {
        this.head=heap.memoryBlockFromHandle(address);
    }
    public long getHead()
    {
        return head.getLong(0);
    }

    public boolean add(TransactionalHeap heap, TableId tableId,int key, long value)
    {
        PMemLLNode tableNode = findOrCreateTableNode(heap, tableId);
        addValue(heap, tableNode, key, value);
        return true; //TODO :Fix return unconditionally
    }

    private PMemLLNode findOrCreateTableNode(TransactionalHeap heap,TableId tableId)//Finds if already exists or creates a new one & returns
    {

        PMemTableNode lastNode = findTable(heap, tableId);
        if (lastNode == null)
        {

            PMemLLNode newNode = new PMemTableNode(heap, tableId);
            head.setLong(0,newNode.getAddress());
            return newNode;
        }
        TableId tableNodeId = lastNode.getTableIdFromBlock(heap);
        if (tableNodeId.equals(tableId))
        {
            return lastNode;
        }
        PMemLLNode newNode = new PMemTableNode(heap, tableId);
        lastNode.setNext(newNode.getAddress());
        return newNode;
    }

    private boolean addValue(TransactionalHeap heap, PMemLLNode tableNode,int key, long value)
    {

        long firstHeaderNodeAddr = tableNode.getValue();
        if (firstHeaderNodeAddr == 0) //First entry
        {
            PMemLLNode newNode = new PMemSerializationHeaderNode(heap, key, value);
            tableNode.setValue(newNode.getAddress());
            return true;
        }
        setSerializationHeader(heap, firstHeaderNodeAddr, key, value);
        return true;
    }


    private PMemLLNode setSerializationHeader(TransactionalHeap heap, long firstHeaderNodeAddr, int key, long value)
    {
        PMemSerializationHeaderNode headerNode ;
        do //Find last node
        {
            headerNode = PMemSerializationHeaderNode.reload(heap,firstHeaderNodeAddr);
        }  while((firstHeaderNodeAddr = headerNode.getNext()) != 0);
        //create new node
        PMemLLNode node = new PMemSerializationHeaderNode(heap, key, value);
        headerNode.setNext(node.getAddress());
        return node;
    }

    public PMemTableNode findTable(TransactionalHeap heap,TableId tableId)
    {
        long mbAddr = head.getLong(0);
        if (mbAddr == 0) //First entry
        {
            return null;
        }
        PMemTableNode tableNode = null;
        while(mbAddr != 0)
        {
            tableNode = PMemTableNode.reload(heap,mbAddr);
            TableId tableNodeId = tableNode.getTableIdFromBlock(heap);
            long nextNodeAddr = tableNode.getNext();
            if (tableNodeId.equals(tableId))
            {
                return tableNode;
            }
            if(nextNodeAddr == 0)
            {
                break; //returns last node in the linkedlist
            }
            //continue iterating
            mbAddr = nextNodeAddr;
        }
        return tableNode;
    }


    //ToDO: Need to handle remove
/*    private boolean remove(TableId key)
    {
        return true;
    }
    private boolean remove(TableId tableId, int key)
    {
        return true;
    }*/
}
