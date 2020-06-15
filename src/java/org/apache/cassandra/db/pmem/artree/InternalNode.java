/*
 * Copyright (C) 2018-2020 Intel Corporation
 *
 * SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause
 *
 */

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.function.Consumer;
import java.util.Optional;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public abstract class InternalNode extends Node {
    /*InternalNode(TransactionalHeap heap, long size) {
        super(heap, size);
        //initBlankRadixIndex((byte)0xff);
    }*/

    InternalNode(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    InternalNode(TransactionalHeap heap, long size, Consumer<Range> initializer) {
        super(heap, heap.allocateUnboundedMemoryBlock(size, (Range range) -> {
            range.setByte(BLANK_RADIX_INDEX_OFFSET, (byte)0xff);
            initializer.accept(range);
        })
        );
    }

    // Node256 needs to override this to just check if the address for blank radix child is 0
    boolean hasBlankRadixChild() {
        return getBlankRadixIndex() != (byte)0xff;
    }

    byte getBlankRadixIndex() {
        return mb.getByte(BLANK_RADIX_INDEX_OFFSET);
    }

    void setBlankRadixIndex(byte index) {
        mb.setByte(BLANK_RADIX_INDEX_OFFSET, index);
    }

    short getChildrenCount() {
        return mb.getShort(CHILDREN_COUNT_OFFSET);
    }

    /*void initChildrenCount(short count) {
        mb.setDurableShort(CHILDREN_COUNT_OFFSET, count);
    }*/

    void setChildrenCount(short count) {
        mb.setShort(CHILDREN_COUNT_OFFSET, count);
    }

    void incChildrenCount() {
        setChildrenCount((short)(getChildrenCount() + (short)1));
    }

    void decChildrenCount() {
        setChildrenCount((short)(getChildrenCount() - (short)1));
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
    
    @Override
    void destroy(Consumer<Long> cleaner) {
        // iterate though children and call destroy
        Node child;
        for (int i=0; i<capacity(); i++) {
            if ((child = getChildAtIndex(i)) != null) {
                child.destroy(cleaner); 
                child.free();
            }
        }
    }

    int clearBlankRadixFlag() {
        int index = (int)getBlankRadixIndex();
        if (index != -1) mb.setByte(BLANK_RADIX_INDEX_OFFSET, (byte)0xff);
        return index;
    }

    Byte findLowestRadix(byte radix, boolean visited) {
        //integer version
        int cmp;
        int lowest;
        cmp = visited ? radix : Byte.MIN_VALUE - 1;
        lowest = Byte.MAX_VALUE + 1;
        byte[] radices = getRadices();
        // System.out.println("visited is "+visited+", need to find lowest radix between "+cmp+" and "+lowest);

        for (int i = 0; i < radices.length; i++) {
            // System.out.println("current radix "+radices[i]);
            if (radices[i] > cmp && radices[i] != (byte)0 && radices[i] < lowest) lowest = radices[i];
        }
        if (lowest == Byte.MAX_VALUE + 1) {/*System.out.println("Returning null");*/ return null;} 
        // System.out.println("returning "+lowest);
        return (byte)lowest;
    }

    abstract short capacity();
    abstract byte[] getRadices();
    abstract boolean addChild(byte radix, Node node);
    abstract int findChildIndex(byte radix);
    abstract long findValueAtIndex(int index);
    abstract void putChildAtIndex(int index, Node child);
    abstract InternalNode grow(Node child, Optional<Byte> radix);
    abstract NodeEntry[] getEntries();
    abstract void deleteChild(Byte radix);

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
