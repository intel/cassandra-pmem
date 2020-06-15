/*
 * Copyright (C) 2018-2020 Intel Corporation
 *
 * SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause
 *
 */

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.function.Consumer;

public class SimpleLeaf extends Leaf {
    protected static final long SIZE = Node.HEADER_SIZE + 8L;
    private static final long VALUE_OFFSET = Node.HEADER_SIZE;

    SimpleLeaf(TransactionalHeap heap) {
        super(heap, SIZE);
        initType(Node.SIMPLE_LEAF_TYPE);
    }

    SimpleLeaf(TransactionalHeap heap, long value) {
        this(heap, new byte[]{0}, 0, 0, value);
    }

    SimpleLeaf(TransactionalHeap heap, byte[] prefix, int start, int length, long value) {
        super(heap, heap.allocateUnboundedMemoryBlock(SIZE, (Range range) -> {
            //set type
            range.setByte(Node.NODE_TYPE_OFFSET, Node.SIMPLE_LEAF_TYPE);
            //set prefix
            if (length > 0) {
                range.setInt(Node.PREFIX_LENGTH_OFFSET, length);
                range.copyFromArray(prefix, start, Node.COMPRESSED_PATH_OFFSET, length);
            }
            range.setLong(VALUE_OFFSET, value);
        }));
    }

    //factory method that creates a leaf and prepends internal nodes as needed
    //returns the topmost parent node or leaf
    static Node create(TransactionalHeap heap, byte[] prefix, int start, int length, long value) {
        if (length > Node.MAX_PREFIX_LENGTH) {
            return Leaf.prependNodes(heap, prefix, start, length, value);
        }
        else return new SimpleLeaf(heap, prefix, start, length, value);
    }

    SimpleLeaf(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    long getValue() {
        return mb.getLong(VALUE_OFFSET);
    }

    @Override
    void setValue(long value) {
        mb.setLong(VALUE_OFFSET, value);
    }

    @Override
    void destroy(Consumer<Long> cleaner) {
        cleaner.accept(getValue());
    }
}
