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

public class PMemSerializationHeaderNode extends PMemLLNode
{
    private static final int HEADER_VALUE_OFFSET = 4;
    private static final int NEXT_NODE_OFFSET = 12;
    int position = 0;
    private static final int size =  Integer.BYTES + Long.BYTES + Long.BYTES;

    //TODO explore withRange
    PMemSerializationHeaderNode(TransactionalHeap heap, int key, long value)
    {
        super(heap, size);
        mb.setInt(position, key);
        position += Integer.BYTES;
        mb.setLong(position, value);
        position += Long.BYTES;
        mb.setLong(position,0);
    }

    PMemSerializationHeaderNode(TransactionalHeap heap, TransactionalMemoryBlock memoryBlock, long value, long nextNodeOffset)
    {
        super(heap, memoryBlock);
        this.value = value;
        this.next = nextNodeOffset;
    }


    public static PMemSerializationHeaderNode reload(TransactionalHeap heap, long mbAddr)
    {
        TransactionalMemoryBlock mb = heap.memoryBlockFromHandle(mbAddr);
        long value = mb.getLong(HEADER_VALUE_OFFSET) ;
        long nextNodeOffset = mb.getLong(NEXT_NODE_OFFSET) ;
        return new PMemSerializationHeaderNode(heap, mb, value, nextNodeOffset);
    }
    public long getValue()
    {
        return value;
    }

    public void setValue(long value)
    {
        mb.setLong(HEADER_VALUE_OFFSET,value);
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

    public int getKey()
    {
        return mb.getInt(0);
    }
}
