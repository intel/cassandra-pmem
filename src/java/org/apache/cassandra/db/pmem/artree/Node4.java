// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.Arrays;

public class Node4 extends InternalNode {
    private static final long SIZE = Node.HEADER_SIZE + 4L * (1L + 8L);
    static final long RADIX_OFFSET = Node.HEADER_SIZE + (4L * 8L);
    static final long CHILDREN_OFFSET = Node.HEADER_SIZE;
    private static final int  MAX_CAPACITY = 4;

    Node4(Heap heap) {
        super(heap, SIZE);
        initType(Node.NODE4_TYPE);
    }

    Node4(Heap heap, MemoryBlock<Raw> mb) {
        super(heap, mb);
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
            entries[index++] = new NodeEntry((byte)0, (Leaf)getChildAtIndex(blankIndex=getBlankRadixIndex()));
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
        return new Node16(heap, this);
    }
}
