// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public class SimpleLeaf extends Leaf {
    private static final long SIZE = Node.HEADER_SIZE + 8L;
    private static final long VALUE_OFFSET = Node.HEADER_SIZE;

    SimpleLeaf(Heap heap) {
        super(heap, SIZE);
        initType(Node.SIMPLE_LEAF_TYPE);
    }

    SimpleLeaf(Heap heap, Object value) {
        super(heap, SIZE);
        initType(Node.SIMPLE_LEAF_TYPE);
    }

    SimpleLeaf(Heap heap, long value) {
        super(heap, SIZE);
        initType(Node.SIMPLE_LEAF_TYPE);
        initValue(value);
    }

    SimpleLeaf(Heap heap, MemoryBlock<Unbounded> mb) {
        super(heap, mb);
    }

    void initValue(long value) {
        mb.setDurableLong(VALUE_OFFSET, value);
    }

    long getValue() {
        return mb.getLong(VALUE_OFFSET);
    }

    @Override
    void setValue(long value) {
        mb.setTransactionalLong(VALUE_OFFSET, value);
    }
}
