// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public class Node48 extends InternalNode {
    private static final int  MAX_CAPACITY = 48;
    private static final int  MAX_RADICES = 256;

    private static final long SIZE = Node.HEADER_SIZE + 1L * (long)MAX_RADICES + (long)(Long.BYTES * MAX_CAPACITY);
    private static final long RADIX_OFFSET = Node.HEADER_SIZE;
    static final long CHILDREN_OFFSET = Node.HEADER_SIZE + 256L;

    // Special design to handle zero being both init value and a valid index:
    // the indices to the children will be 1-based, so an index of 0 is invalid
    private byte[] radices;

    Node48(Heap heap) {
        super(heap, SIZE);
        initType(Node.NODE48_TYPE);
    }

    Node48(Heap heap, MemoryBlock<Unbounded> mb) {
        super(heap, mb);
    }

    Node48(Heap heap, Node16 oldNode) {
        this(heap);
        // offset is 1 to skip the TYPE field that's already set
        heap.copyMemory(oldNode.mb, 1, this.mb, 1, Node.HEADER_SIZE - 1);
        int blankRadixIndex = oldNode.getBlankRadixIndex();
        byte[] oldRadices = oldNode.getRadices();
        for (int index = 0; index < oldRadices.length; index++) {
            // Skip the blank child radix, otherwise it will map the 0th
            // index to the blank child when it's not supposed to
            if (index != blankRadixIndex)
                addRadix((byte)(index+1), (int)oldRadices[index]);
        }
        heap.copyMemory(oldNode.mb, Node16.CHILDREN_OFFSET, this.mb, Node48.CHILDREN_OFFSET, oldNode.capacity() * Long.BYTES);
        this.mb.flush(0, this.SIZE);
    }

    @Override
    byte[] getRadices() {
        if (radices == null) {
            byte[] radices = new byte[MAX_RADICES];
            heap.copyToArray(mb, RADIX_OFFSET, radices, 0, MAX_RADICES);
            this.radices = radices;
        }
        return this.radices;
    }

    void addRadix(byte radix, int index) {
        mb.setTransactionalByte(RADIX_OFFSET + index + 128, radix);
    }

    @Override
    NodeEntry[] getEntries() {
        byte[] radices = getRadices();
        NodeEntry[] entries = new NodeEntry[getChildrenCount()];
        int blankIndex=-1;
        int index=0;
        boolean hasBlank = hasBlankRadixChild();
        if (hasBlank) {
            entries[index++] = new NodeEntry((byte)0, getChildAtIndex(blankIndex=getBlankRadixIndex()));
        }
        for (int i=0; i < MAX_RADICES; i++) {
            if (radices[(int)i] != blankIndex && radices[(int)i] !=0) entries[index++] = new NodeEntry((byte)(i-128), getChildAtIndex(radices[i]-1));
        }
        return entries;
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
                // For Node48, the radix field is an index to the child,
                // and the radix itself is used a the index into the radix field
                addRadix((byte)(index + 1), (int)radix);   // +1 for 1-based indices
            }
        }
        putChildAtIndex(index, node);
        return true;
    }

    @Override
    int findChildIndex(byte radix) {
        byte[] radices = getRadices();
        if (radices[(int)radix + 128] != 0) return (int)(radices[(int)radix + 128]) - 1;   // 1-based
        else return -1;
    }

    @Override
    long findValueAtIndex(int index) {
        if (index == -1) return 0;
        return mb.getLong(CHILDREN_OFFSET + Long.BYTES * index);
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
//        System.out.println("needs to grow 48 to 256");
        return new Node256(heap, this);
    }

    @Override
    void printChildren(StringBuilder start, int depth) {
        byte[] radices = getRadices();
        Node node;
        for (int i = 0; i < radices.length; i++) {
            if (radices[i] != 0) {
                // System.out.println(start + "For radix " + String.format("%02X ", radices[i]) + ":");
                System.out.println(start + "For radix " + new String(new byte[]{(byte)(i-128)}) + "(" + (i-128) + "):");
                node = getChildAtIndex(radices[i]-1);
                if(node != null)
                {
                    node.print(depth + 1);
                }
            }
        }
    }
}
