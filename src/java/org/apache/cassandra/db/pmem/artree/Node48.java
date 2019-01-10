// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.Optional;

public class Node48 extends InternalNode {
    private static final int  MAX_CAPACITY = 48;
    private static final int  MAX_RADICES = 256;

    protected static final long SIZE = Node.HEADER_SIZE + 1L * (long)MAX_RADICES + (long)(Long.BYTES * MAX_CAPACITY);
    private static final long RADIX_OFFSET = Node.HEADER_SIZE;
    static final long CHILDREN_OFFSET = Node.HEADER_SIZE + 256L;

    // Special design to handle zero being both init value and a valid index:
    // the indices to the children will be 1-based, so an index of 0 is invalid
    private byte[] radices;

    /*Node48(TransactionalHeap heap) {
        super(heap, SIZE);
        //initType(Node.NODE48_TYPE);
    }*/

    Node48(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    Node48(TransactionalHeap heap, Node16 oldNode, Node newNode, Optional<Byte> radix) {
        super(heap, SIZE, (Range range) -> {
        // offset is 1 to skip the TYPE field that's already set
            range.setByte(Node.NODE_TYPE_OFFSET, Node.NODE48_TYPE);
            range.copyFromMemoryBlock(oldNode.mb, 1, 1, Node.HEADER_SIZE - 1);
            int blankRadixIndex = oldNode.getBlankRadixIndex();
            byte[] oldRadices = oldNode.getRadices();
            for (int index = 0; index < oldRadices.length; index++) {
                // Skip the blank child radix, otherwise it will map the 0th
                // index to the blank child when it's not supposed to
                if (index != blankRadixIndex)
                    range.setByte(RADIX_OFFSET + (int)oldRadices[index] + 128, (byte)(index + 1));
            }
            range.copyFromMemoryBlock(oldNode.mb, Node16.CHILDREN_OFFSET, Node48.CHILDREN_OFFSET, oldNode.capacity() * Long.BYTES);
            //set radix
            if (radix.isPresent()) range.setByte(RADIX_OFFSET + (int)radix.get() + 128, (byte)(16 + 1));
            else range.setByte(Node.BLANK_RADIX_INDEX_OFFSET, (byte)16);
            //set value
            range.setLong(CHILDREN_OFFSET + 16 * Long.BYTES, newNode.address());
            //set childrencount
            range.setShort(InternalNode.CHILDREN_COUNT_OFFSET, (short)(16 + 1));
        });
    }

    @Override
    byte[] getRadices() {
        if (radices == null) {
            byte[] radices = new byte[MAX_RADICES];
            mb.copyToArray(RADIX_OFFSET, radices, 0, MAX_RADICES);
            this.radices = radices;
        }
        return this.radices;
    }

    void addRadix(byte radix, int index) {
        mb.setByte(RADIX_OFFSET + index + 128, radix);
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
    
    void deleteChild(Byte radix) {
        int index;
        if (radix == null) {
            index = clearBlankRadixFlag();
        }
        else index = findChildIndex(radix);
        if (index != -1) {
            //mb.addToTransaction(0, this.SIZE);
            mb.withRange(0, this.SIZE, (Range range) ->{
                if (radix != null) {
                    //delete radix
                    range.setByte(RADIX_OFFSET + (int)radix + 128, (byte)0);
                }
                //overwrite child value with the most recent child
                decChildrenCount();
                int childrenCount = getChildrenCount();
                if (childrenCount == index) {
                    range.setLong(CHILDREN_OFFSET + childrenCount * Long.BYTES, 0L);
                } else {
                    range.setLong(CHILDREN_OFFSET + index * Long.BYTES, mb.getLong(CHILDREN_OFFSET + childrenCount * Long.BYTES));
                    range.setLong(CHILDREN_OFFSET + childrenCount * Long.BYTES, 0L);
                    //update radix of most recent child
                    byte[] radices = getRadices();
                    for (int i = 0; i < radices.length; i++) {
                        if ((int)(radices[i]) == childrenCount + 1) {
                            range.setByte(RADIX_OFFSET + i, (byte)(index+1));
                            break;
                        }
                    }
                }
                this.radices = null;
            });
        }
    }
    
    @Override
    Byte findLowestRadix(byte radix, boolean visited) {
        int cmp = visited ? radix : Byte.MIN_VALUE;
        int lowest = Byte.MAX_VALUE;
        // System.out.println("visited is "+visited+", need to find lowest radix between "+cmp+" and "+lowest);
        for (int i=cmp + 1; i<=lowest; i++) {
            if (findChildIndex((byte)i) != -1) {
                // System.out.println("Returning "+(byte)i);
                return (byte)i;
            }
        }
        return null;   
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
        mb.setLong(CHILDREN_OFFSET + index * Long.BYTES, child.address());
    }

    @Override
    short capacity() { return (short)MAX_CAPACITY; }

    @Override
    InternalNode grow(Node child, Optional<Byte> radix) {
//        System.out.println("needs to grow 48 to 256");
        return new Node256(heap, this, child, radix);
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
