// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.function.Consumer;

public class ComplexLeaf extends Leaf {
    private static final long SIZE = Node.HEADER_SIZE + 8L + 8L + 8L;
    private static final long KEY_OFFSET = Node.HEADER_SIZE;
    private static final long VALUE_OFFSET = Node.HEADER_SIZE + 8L;
    private static final long NEXT_OFFSET = Node.HEADER_SIZE + 16L;

    ComplexLeaf(TransactionalHeap heap, long key, long value) {
        super(heap, heap.allocateUnboundedMemoryBlock(SIZE, (Range range) -> {
            //set type
            range.setByte(Node.NODE_TYPE_OFFSET, Node.COMPLEX_LEAF_TYPE);
            range.setLong(KEY_OFFSET, key);
            range.setLong(VALUE_OFFSET, value);
        }));
    }

    ComplexLeaf(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    /*void initKey(long key) {
        mb.setDurableLong(KEY_OFFSET, key);
    }*/

    /*void initValue(long value) {
        mb.setDurableLong(VALUE_OFFSET, value);
    }*/

    long getKey() {
        return mb.getLong(KEY_OFFSET);
    }

    long getValue() {
        return mb.getLong(VALUE_OFFSET);
    }

    @Override
    void setValue(long value) {
        mb.setLong(VALUE_OFFSET, value);
    }

    @SuppressWarnings("unchecked")
    ComplexLeaf getNext() {
        return (ComplexLeaf)Node.rebuild(heap, mb.getLong(NEXT_OFFSET));
    }

    @Override
    void destroy(Consumer<Long> cleaner) {
//TODO not correct yet
        cleaner.accept(getValue());
    }

    ComplexLeaf matches(long decoratedKey) {
        ComplexLeaf curLeaf = this;
        while (curLeaf != null && curLeaf.getKey() != decoratedKey) {
            curLeaf = curLeaf.getNext();
        }
        return curLeaf;
    }
}
