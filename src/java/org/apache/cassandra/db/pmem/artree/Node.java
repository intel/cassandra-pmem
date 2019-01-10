// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import lib.llpl.*;

public abstract class Node {
    static final byte NODE4_TYPE = 0;
    static final byte NODE16_TYPE = 1;
    static final byte NODE48_TYPE = 2;
    static final byte NODE256_TYPE = 3;
    static final byte SIMPLE_LEAF_TYPE = 4;
    static final byte COMPLEX_LEAF_TYPE = 5;
    static final byte ROOT_TYPE = 6;

    protected static final long HEADER_SIZE = 16L;
    protected static final long NODE_TYPE_OFFSET = 0L;
    protected static final long BLANK_RADIX_INDEX_OFFSET = 1L;
    protected static final long CHILDREN_COUNT_OFFSET = 2L;
    protected static final long PREFIX_LENGTH_OFFSET = 4L;
    protected static final long COMPRESSED_PATH_OFFSET = 8L;
    static final int MAX_PREFIX_LENGTH = 8;

    TransactionalHeap heap;
    TransactionalUnboundedMemoryBlock mb;

    Node(TransactionalHeap heap, long size) {
        this.heap = heap;
        this.mb = this.heap.allocateUnboundedMemoryBlock(size);
    }

    Node(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        this.heap = heap;
        this.mb = mb;
    }

    static Node rebuild(TransactionalHeap heap, long handle) {
        if (handle == 0) return null;
        TransactionalUnboundedMemoryBlock mb = heap.unboundedMemoryBlockFromHandle(handle);
        Node ret = null;
        byte rawType = mb.getByte(NODE_TYPE_OFFSET);
        switch (rawType) {
            case NODE4_TYPE: ret = new Node4(heap, mb); break;
            case NODE16_TYPE: ret = new Node16(heap, mb); break;
            case NODE48_TYPE: ret = new Node48(heap, mb); break;
            case NODE256_TYPE: ret = new Node256(heap, mb); break;
            case SIMPLE_LEAF_TYPE: ret = new SimpleLeaf(heap, mb); break;
            case COMPLEX_LEAF_TYPE: ret = new ComplexLeaf(heap, mb); break;
            case ROOT_TYPE: ret = new Root(heap, mb); break;
            default: break;
        }
        return ret;
    }

    void free() {
        mb.free();
    }

    long address() {
        return mb.handle();
    }

    byte getType() {
        return mb.getByte(NODE_TYPE_OFFSET);
    }

    void initType(byte type) {
        mb.setByte(NODE_TYPE_OFFSET, type);
    }

    void setType(byte type) {
        mb.setByte(NODE_TYPE_OFFSET, type);
    }

    int getPrefixLength() {
        return mb.getInt(PREFIX_LENGTH_OFFSET);
    }

    void setPrefixLength(int length) {
        mb.setInt(PREFIX_LENGTH_OFFSET, length);
    }

    byte[] getPrefix() {
        byte[] prefix = new byte[getPrefixLength()];
        mb.copyToArray(COMPRESSED_PATH_OFFSET, prefix, 0, prefix.length);
        return prefix;
    }

    void initPrefix(byte[] prefix) {
       // if (prefix == null) mb.setDurableLong(COMPRESSED_PATH_OFFSET, 0);//TODO: Remove after analysis
        if (prefix.length <= 8) {
            //MemoryBlock.durableCopyFromArray(prefix, 0, mb.handle() + COMPRESSED_PATH_OFFSET, prefix.length);//TODO: Remove after analysis
            mb.copyFromArray(prefix, 0, COMPRESSED_PATH_OFFSET, prefix.length);
            //mb.flush(COMPRESSED_PATH_OFFSET, prefix.length);
        } else {
            //mb.setDurableLong(COMPRESSED_PATH_OFFSET, prefix.length);
            throw new IllegalArgumentException("Prefix more than 8 bytes");
        }
    }

    void setPrefix(byte[] prefix) {
       // if (prefix == null) mb.setTransactionalLong(COMPRESSED_PATH_OFFSET, 0);//TODO: Remove after analysis
        if (prefix.length <= 8) {
            mb.copyFromArray(prefix, 0, COMPRESSED_PATH_OFFSET, prefix.length);
            //heap.copyFromArray(prefix, 0, mb, COMPRESSED_PATH_OFFSET, prefix.length);
        } else {
            //mb.setTransactionalLong(COMPRESSED_PATH_OFFSET, prefix.length);
            throw new IllegalArgumentException("Prefix more than 8 bytes");
        }
    }

    void updatePrefix(byte[] prefix, int start, int updatedLength) {
        if (updatedLength <= 0) {
            setPrefixLength(0);
            return;
        }
        byte[] updatedPrefix = new byte[8];
        for (int i = 0; i < updatedLength; i++) {
            updatedPrefix[i] = prefix[start + i];
        }
        setPrefixLength(updatedLength);
        setPrefix(updatedPrefix);
    }

    int checkPrefix(byte[] key, int depth) {
        byte[] prefix = getPrefix();
        int i = 0;
        while (i < (key.length-depth) && i < prefix.length && key[depth + i] == prefix[i]) i++;
        return i;
    }

    boolean isLeaf() {
        return false;
    }

    String getPrefixString() {
        StringBuilder sb = new StringBuilder();
        byte[] prefix = getPrefix();
        for (int i = 0; i < prefix.length; i++) {
            sb.append(String.format("%02X ", prefix[i]));
        }
        return sb.toString();
        //return new String(prefix);
    }

    abstract void destroy(Consumer<Long> cleaner); 

    void print(int depth) {
        StringBuilder start = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            start.append("   ");
        }
        System.out.println(start + "=========================");
        System.out.println(start + "Type: " + getType());
        System.out.println(start + "Prefix length: " + getPrefixLength());
        System.out.println(start + "Prefix: " + getPrefixString());
        if (!isLeaf()) {
            InternalNode intNode = (InternalNode)this;
            System.out.println(start + "Child count: " + intNode.getChildrenCount());
            if (intNode.hasBlankRadixChild()) {
                System.out.println(start + "Has Blank Radix Child at " + intNode.getBlankRadixIndex());
            }
            intNode.printChildren(start, depth);
        } else {
            SimpleLeaf leaf = (SimpleLeaf)this;
            //System.out.println(start + "Key: " + Long.toHexString(leaf.getKey()));
            System.out.println(start + "Value: " + Long.toHexString(leaf.getValue()));
            /*if (leaf.getNext() != null) {
                leaf.getNext().print(depth+1);
            }*/
        }
    }


}
