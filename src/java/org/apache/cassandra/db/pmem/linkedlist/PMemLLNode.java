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

public abstract class PMemLLNode
{
    long value;
    long next;
    TransactionalMemoryBlock mb;
    int position;
    TransactionalHeap heap;

    PMemLLNode(TransactionalHeap heap, long size)
    {
        this.heap = heap;
        this.mb = heap.allocateMemoryBlock(size);
    }

    PMemLLNode(TransactionalHeap heap, TransactionalMemoryBlock mb)
    {
        this.heap = heap;
        this.mb = mb;
    }
    long getAddress()
    {
        return mb.handle();
    }


    abstract long getValue();
    abstract void setValue(long value);
    abstract long getNext();
    abstract void setNext(long next);
}

