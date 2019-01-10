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

public class PMemTableNode extends PMemLLNode
{
    private static final int TABLE_ID_STRING_SIZE = 36;
    private static final int TABLE_VALUE_OFFSET = 36;
    private static final int NEXT_NODE_OFFSET = 44;
    private static final int size =  TABLE_ID_STRING_SIZE + Long.BYTES + Long.BYTES;

    PMemTableNode(TransactionalHeap heap, TableId key)
    {
        super(heap, size);
        //create the new node
        byte[] tableUUIDBytes = key.toString().getBytes();
        mb.copyFromArray(tableUUIDBytes, 0, position, tableUUIDBytes.length);
        position += TABLE_ID_STRING_SIZE;
        mb.setLong(position, value);
        position += Long.BYTES;
        mb.setLong(position,0);
    }

    PMemTableNode(TransactionalHeap heap, TransactionalMemoryBlock memoryBlock, long value, long nextNodeOffset)
    {
        super(heap, memoryBlock);
        this.value = value;
        this.next = nextNodeOffset;
    }

    public static PMemTableNode reload(TransactionalHeap heap, long mbAddr)
    {
        TransactionalMemoryBlock mb = heap.memoryBlockFromHandle(mbAddr);
        long value = mb.getLong(TABLE_ID_STRING_SIZE) ;
        long nextNodeOffset = mb.getLong(NEXT_NODE_OFFSET) ;
        return new PMemTableNode(heap, mb, value, nextNodeOffset);
    }
    public long getValue()
    {
        return value;
    }

    public void setValue(long value)
    {
        mb.setLong(TABLE_VALUE_OFFSET,value);
        this.value = value ;
    }
    public long getNext()
    {
        return next;
    }

    public void setNext(long next)
    {
        mb.setLong(NEXT_NODE_OFFSET,next);
        this.next = next;
    }

    public TableId getKey()
    {
        return getTableIdFromBlock(heap);
    }

    public TableId getTableIdFromBlock(TransactionalHeap heap)
    {
        byte[] b = new byte[TABLE_ID_STRING_SIZE];
        mb.copyToArray(0, b,0,TABLE_ID_STRING_SIZE);
        String str = new String(b);
        TableId tableId = TableId.fromString(str);
        return tableId;
    }

}
