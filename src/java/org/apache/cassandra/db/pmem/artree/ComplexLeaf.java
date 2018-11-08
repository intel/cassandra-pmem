// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public class ComplexLeaf extends Leaf {
    private static final long SIZE = Node.HEADER_SIZE + 8L + 8L + 8L;
    private static final long KEY_OFFSET = Node.HEADER_SIZE;
    private static final long VALUE_OFFSET = Node.HEADER_SIZE + 8L;
    private static final long NEXT_OFFSET = Node.HEADER_SIZE + 16L;

    ComplexLeaf(Heap heap, long key, long value) {
        super(heap, SIZE);
        initType(Node.COMPLEX_LEAF_TYPE);
        initKey(key);
        initValue(value);
    }

    ComplexLeaf(Heap heap, MemoryBlock<Unbounded> mb) {
        super(heap, mb);
    }

    void initKey(long key) {
        mb.setDurableLong(KEY_OFFSET, key);
    }

    void initValue(long value) {
        mb.setDurableLong(VALUE_OFFSET, value);
    }

    long getKey() {
        return mb.getLong(KEY_OFFSET);
    }

    long getValue() {
        return mb.getLong(VALUE_OFFSET);
    }

    @Override
    void setValue(long value) {
        mb.setTransactionalLong(VALUE_OFFSET, value);
    }

    @SuppressWarnings("unchecked")
    ComplexLeaf getNext() {
        return (ComplexLeaf)Node.rebuild(heap, mb.getLong(NEXT_OFFSET));
    }

    ComplexLeaf matches(long decoratedKey) {
        ComplexLeaf curLeaf = this;
        while (curLeaf != null && curLeaf.getKey() != decoratedKey) {
            curLeaf = curLeaf.getNext();
        }
        return curLeaf;
    }
}
