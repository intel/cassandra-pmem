// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.Arrays;

public class Node16 extends InternalNode {
    private static final long SIZE = Node.HEADER_SIZE + 16L * (1L + 8L);
    private static final long RADIX_OFFSET = Node.HEADER_SIZE;
    static final long CHILDREN_OFFSET = Node.HEADER_SIZE + 16L;
    private static final int  MAX_CAPACITY = 16;

    Node16(Heap heap) {
        super(heap, SIZE);
        initType(Node.NODE16_TYPE);
    }

    Node16(Heap heap, MemoryBlock<Raw> mb) {
        super(heap, mb);
    }

    Node16(Heap heap, Node4 oldNode) {
        this(heap);
        // offset is 1 to skip the TYPE field that's already set
        heap.copyMemory(oldNode.mb, 1, this.mb, 1, Node.HEADER_SIZE - 1);
        heap.copyMemory(oldNode.mb, Node4.RADIX_OFFSET, this.mb, RADIX_OFFSET, oldNode.capacity());
        heap.copyMemory(oldNode.mb, Node4.CHILDREN_OFFSET, this.mb, Node16.CHILDREN_OFFSET, oldNode.capacity() * Long.BYTES);
        this.mb.flush(0, this.SIZE);
    }

    @Override
    byte[] getRadices() {
        byte[] ret = new byte[MAX_CAPACITY];
        heap.copyToArray(mb, RADIX_OFFSET, ret, 0, MAX_CAPACITY);
        return ret;
    }

    NodeEntry[] getEntries() {
        byte[] radices = getRadices();
        NodeEntry[] entries = new NodeEntry[getChildrenCount()];
        int blankIndex=-1;
        int index=0;
        boolean hasBlank = hasBlankRadixChild();
        if (hasBlank) {
            entries[index++] = new NodeEntry((byte)0, getChildAtIndex(blankIndex=getBlankRadixIndex()));
        }
        for (int i=0; i < entries.length; i++) {
            if (i != blankIndex) entries[index++] = new NodeEntry(radices[i], getChildAtIndex(i));
        }
        Arrays.sort(entries, (hasBlank) ? 1 : 0, entries.length, (x, y)-> Byte.compare(x.radix, y.radix));
        return entries;
    }

    void addRadix(byte radix, int index) {
        mb.setTransactionalByte(RADIX_OFFSET + index, radix);
    }

    @Override
    boolean addChild(byte radix, Node node) {
        int index = findChildIndex(radix);
        if (index == -1) {
            if (getChildrenCount() >= MAX_CAPACITY) {
                return false;   // need to grow, out of capacity
            } else {
                index = getChildrenCount();
                incChildrenCount();
                addRadix(radix, index);
            }
        }
        putChildAtIndex(index, node);
        return true;
    }

    // TODO: make vectorized
    @Override
    int findChildIndex(byte radix) {
        byte[] radices = getRadices();
        for (int i = 0; i < getChildrenCount(); i++) {
            if (radices[i] == radix)
                return i;
        }
        return -1;
    }

    @Override
    long findValueAtIndex(int index) {
        if (index == -1) return 0;  // 0 == NULL
        return mb.getLong(CHILDREN_OFFSET + index * 8);
    }

    @Override
    void putChildAtIndex(int index, Node child) {
        if (index == -1) return;
        mb.setTransactionalLong(CHILDREN_OFFSET + index * Long.BYTES, child.address());
    }

    @Override
    short capacity() { return (short)MAX_CAPACITY; }

    @Override
    InternalNode grow() {
        return new Node48(heap, this);
    }
}
