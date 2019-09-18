// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.Optional;
import java.util.Arrays;

public class Node16 extends InternalNode {
    protected static final long SIZE = Node.HEADER_SIZE + 16L * (1L + 8L);
    private static final long RADIX_OFFSET = Node.HEADER_SIZE;
    static final long CHILDREN_OFFSET = Node.HEADER_SIZE + 16L;
    private static final int  MAX_CAPACITY = 16;

    Node16(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    Node16(TransactionalHeap heap, Node4 oldNode, Node newNode, Optional<Byte> radix) {
        super(heap, SIZE, (Range range) -> {
        // offset is 1 to skip the TYPE field that's already set
            range.setByte(NODE_TYPE_OFFSET, Node.NODE16_TYPE);
            range.copyFromMemoryBlock(oldNode.mb, 1, 1, Node.HEADER_SIZE - 1);
            range.copyFromMemoryBlock(oldNode.mb, Node4.RADIX_OFFSET, RADIX_OFFSET, oldNode.capacity());
            range.copyFromMemoryBlock(oldNode.mb, Node4.CHILDREN_OFFSET, Node16.CHILDREN_OFFSET, oldNode.capacity() * Long.BYTES);

            //set radix
            if (radix.isPresent()) range.setByte(RADIX_OFFSET + 4, radix.get());
            else range.setByte(Node.BLANK_RADIX_INDEX_OFFSET, (byte)4);
            //set value
            range.setLong(CHILDREN_OFFSET + 4 * Long.BYTES, newNode.address()); 
            //set childrencount
            range.setShort(InternalNode.CHILDREN_COUNT_OFFSET, (short)(4 + 1));
        });
    }

    @Override
    byte[] getRadices() {
        byte[] ret = new byte[MAX_CAPACITY];
        mb.copyToArray(RADIX_OFFSET, ret, 0, MAX_CAPACITY);
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
        Arrays.sort(entries, (hasBlank) ? 1 : 0, entries.length, (x, y)-> Integer.compareUnsigned(Byte.toUnsignedInt(x.radix), Byte.toUnsignedInt(y.radix)));
        return entries;
    }

    void addRadix(byte radix, int index) {
        mb.setByte(RADIX_OFFSET + index, radix);
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

    void deleteChild(Byte radix) {
        int index;
        if (radix == null) {
            //blankRadix
            index = clearBlankRadixFlag();
        }
        else index = findChildIndex(radix);

        if (index != -1) {
            mb.withRange(Node.HEADER_SIZE, this.SIZE - Node.HEADER_SIZE, (Range range) -> {
                //delete radix
                range.copyFromMemoryBlock(mb, RADIX_OFFSET + index + 1, RADIX_OFFSET + index, MAX_CAPACITY - index - 1);        
                range.setByte(RADIX_OFFSET + MAX_CAPACITY - 1, (byte)0);
                //delete value
                range.copyFromMemoryBlock(mb, CHILDREN_OFFSET + ((index + 1L) * 8L), CHILDREN_OFFSET + (index * 8L), (MAX_CAPACITY - index - 1) * 8L);        
                range.setLong(CHILDREN_OFFSET + (MAX_CAPACITY - 1) * Long.BYTES, 0L);
                decChildrenCount();
            });
        }
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
        mb.setLong(CHILDREN_OFFSET + index * Long.BYTES, child.address());
    }

    @Override
    short capacity() { return (short)MAX_CAPACITY; }

    @Override
    InternalNode grow(Node child, Optional<Byte> radix) {
        return new Node48(heap, this, child, radix);
    }
}
