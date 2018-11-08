// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public abstract class Leaf extends Node {
    Leaf(Heap heap, MemoryBlock<Unbounded> mb) {
        super(heap, mb);
    }

    Leaf(Heap heap, long size) {
        super(heap, size);
    }

    abstract void setValue(long value);
    abstract long getValue();

    @Override
    boolean isLeaf() {
        return true;
    }

    Node prependNodes(byte[] key, int start, int length) {
        Node child = this;
        int curStart = key.length - 8;
        int curLength = length;
        byte[] childPrefix = new byte[Node.MAX_PREFIX_LENGTH];

        while (curLength > Node.MAX_PREFIX_LENGTH) {
      //      System.out.println("curLength is " + curLength);
            InternalNode parent = new Node4(heap);
            System.arraycopy(key, curStart, childPrefix, 0, Node.MAX_PREFIX_LENGTH);
            child.initPrefixLength(Node.MAX_PREFIX_LENGTH);
            child.initPrefix(childPrefix);
            parent.addChild(key[--curStart], child);
            child = parent;
            curStart -= Node.MAX_PREFIX_LENGTH;
            curLength -= (Node.MAX_PREFIX_LENGTH + 1);
        }
        //    System.out.println("curLength is " + curLength);

        System.arraycopy(key, start, childPrefix, 0, curLength);
        child.initPrefixLength(curLength);
        child.initPrefix(childPrefix);

        return child;
    }

}
