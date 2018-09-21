// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public class Node256 extends InternalNode {
    // 257 for Node256: 1 more slot for the blank radix child
    private static final long SIZE = Node.HEADER_SIZE + 257L * 8L;
    private static final long CHILDREN_OFFSET = Node.HEADER_SIZE;
    private static final int  BLANK_RADIX_CHILD_INDEX = 256;
    private static final int  MAX_CAPACITY = 257;

    Node256(Heap heap) {
        super(heap, SIZE);
        initType(Node.NODE256_TYPE);
    }

    Node256(Heap heap, MemoryBlock<Raw> mb) {
        super(heap, mb);
    }

    Node256(Heap heap, Node48 oldNode) {
        this(heap);
        // offset is 1 to skip the TYPE field that's already set
        heap.copyMemory(oldNode.mb, 1, this.mb, 1, Node.HEADER_SIZE - 1);
        byte[] oldRadices = oldNode.getRadices();
        for (int index = 0; index < oldRadices.length; index++) {
            if (oldRadices[index] != 0) {
                initValueAtIndex(index, oldNode.findValueAtIndex((int)(oldRadices[index] - 1)));
            }
        }
        if (oldNode.hasBlankRadixChild()) {
            initValueAtIndex(BLANK_RADIX_CHILD_INDEX, oldNode.findValueAtIndex(oldNode.getBlankRadixIndex()));
        }
        this.mb.flush(0, this.SIZE);
    }

    @Override
    boolean hasBlankRadixChild() {
        return (findValueAtIndex(BLANK_RADIX_CHILD_INDEX) != 0);
    }

    @Override
    boolean addBlankRadixChild(Leaf child) {
        if (hasBlankRadixChild()) {
            findBlankRadixChild().setValue(child.getValue());
            // TODO: free incoming child
        } else {
            putChildAtIndex(BLANK_RADIX_CHILD_INDEX, child);
        }
        return true;
    }

    @Override
    byte[] getRadices() {
        return null;    // does not store radices
    }

    @Override
    NodeEntry[] getEntries() {
        NodeEntry[] entries = new NodeEntry[getChildrenCount()];
        int index=0;
        boolean hasBlank = hasBlankRadixChild();
        if (hasBlank) {
            entries[index++] = new NodeEntry((byte)0, getChildAtIndex(BLANK_RADIX_CHILD_INDEX));
        }
        Node n;
        for (int i=0; i < 256; i++) {
            if (findValueAtIndex(i) != 0) entries[index++] = new NodeEntry((byte)(i-128), getChildAtIndex(i));
        }
        return entries;
    }

    @Override
    boolean addChild(byte radix, Node node) {
        int index = findChildIndex(radix);
        if (findValueAtIndex(index) == 0) {
            incChildrenCount();
        }
        putChildAtIndex(index, node);
        return true;
    }

    @Override
    int findChildIndex(byte radix) {
        return (int)radix + 128;
    }

    @Override
    long findValueAtIndex(int index) {
        if (index == -1) return 0;
        return mb.getLong(CHILDREN_OFFSET + index * Long.BYTES);
    }

    @Override
    void putChildAtIndex(int index, Node child) {
        if (index == -1) return;
        mb.setTransactionalLong(CHILDREN_OFFSET + index * Long.BYTES, child.address());
    }

    private void initValueAtIndex(int index, long value) {
        mb.setDurableLong(CHILDREN_OFFSET + index * Long.BYTES, value);
    }

    @Override
    short capacity() { return (short)MAX_CAPACITY; }

    @Override
    InternalNode grow() { return null; }

    @Override
    void printChildren(StringBuilder start, int depth) {
        for (int i = 0; i < capacity(); i++) {
            if (findValueAtIndex(i) != 0) {
                System.out.println(start + "For radix " + new String(new byte[]{(byte)(i-128)}) + "(" + (i-128) + "):");
                Node node = getChildAtIndex(i);
                if( node != null) {
                    node.print(depth + 1);
                }
            }
        }
    }

}
