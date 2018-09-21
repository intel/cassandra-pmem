// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.nio.ByteBuffer;

public abstract class InternalNode extends Node {
    InternalNode(Heap heap, long size) {
        super(heap, size);
        initBlankRadixIndex((byte)0xff);
    }

    InternalNode(Heap heap, MemoryBlock<Raw> mb) {
        super(heap, mb);
    }

    // Node256 needs to override this to just check if the address for blank radix child is 0
    boolean hasBlankRadixChild() {
        return getBlankRadixIndex() != (byte)0xff;
    }

    byte getBlankRadixIndex() {
        return mb.getByte(BLANK_RADIX_INDEX_OFFSET);
    }

    void initBlankRadixIndex(byte index) {
        mb.setDurableByte(BLANK_RADIX_INDEX_OFFSET, index);
    }

    void setBlankRadixIndex(byte index) {
        mb.setTransactionalByte(BLANK_RADIX_INDEX_OFFSET, index);
    }

    short getChildrenCount() {
        return mb.getShort(CHILDREN_COUNT_OFFSET);
    }

    void initChildrenCount(short count) {
        mb.setDurableShort(CHILDREN_COUNT_OFFSET, count);
    }

    void setChildrenCount(short count) {
        mb.setTransactionalShort(CHILDREN_COUNT_OFFSET, count);
    }

    void incChildrenCount() {
        setChildrenCount((short)(getChildrenCount() + (short)1));
    }

    @SuppressWarnings("unchecked")
    Leaf findBlankRadixChild() {
        return (Leaf)getChildAtIndex(getBlankRadixIndex());
    }

    // Node256 needs to override to avoid overflowing
    boolean addBlankRadixChild(Leaf child) {
        short childrenCount = getChildrenCount();
        if (childrenCount == capacity()) {
            return false;
        }
    /*    if (hasBlankRadixChild()) {
            findBlankRadixChild().setValue(child.getValue());
            child.free();
        }*/ else {
            incChildrenCount();
            setBlankRadixIndex((byte)childrenCount);
            putChildAtIndex(childrenCount, child);
        }
        return true;
    }

    Node findChild(byte radix) {
        return getChildAtIndex(findChildIndex(radix));
    }

    Node getChildAtIndex(int index) {
        return Node.rebuild(heap, findValueAtIndex(index));
    }

    abstract short capacity();
    abstract byte[] getRadices();
    abstract boolean addChild(byte radix, Node node);
    abstract int findChildIndex(byte radix);
    abstract long findValueAtIndex(int index);
    abstract void putChildAtIndex(int index, Node child);
    abstract InternalNode grow();
    abstract NodeEntry[] getEntries();

    boolean isLeaf() {
        return false;
    }


    void printChildren(StringBuilder start, int depth) {
        byte[] radices = getRadices();
        Node node;
        for (int i = 0; i < getChildrenCount(); i++) {
            if (radices != null) {
                // System.out.println(start + "For radix " + String.format("%02X ", radices[i]) + ":");
                System.out.println(start + "For radix " + new String(new byte[]{radices[i]}) + "(" + radices[i] + "):");
                node = getChildAtIndex(i);
                if(node != null)
                {
                    node.print(depth + 1);
                }
            } else {
            }
        }
    }
}
