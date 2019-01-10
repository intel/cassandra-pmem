// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.function.Consumer;

public abstract class Leaf extends Node {
    Leaf(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    Leaf(TransactionalHeap heap, long size) {
        super(heap, size);
    }

    //abstract void initValue(long value);
    abstract void setValue(long value);
    abstract long getValue();

    /*void initPrefix(byte[] prefix, int start, int updatedLength) {
        if (updatedLength <= 0) {
            setPrefixLength(0);
            return;
        }
        byte[] updatedPrefix = new byte[8];
        for (int i = 0; i < updatedLength; i++) {
            updatedPrefix[i] = prefix[start + i];
        }
        initPrefixLength(updatedLength);
        initPrefix(updatedPrefix);
    }*/
 
    @Override
    boolean isLeaf() {
        return true;
    }
    
    /*Node prependNodes(byte[] key, int start, int length) {
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
    }*/

    static Node prependNodes(TransactionalHeap heap, byte[] key, int start, int length, long value) {
        int curStart = key.length - 8;
        int curLength = length;

        //first create leaf
        Node child = new SimpleLeaf(heap, key, curStart, Node.MAX_PREFIX_LENGTH, value); 
        curLength -= (Node.MAX_PREFIX_LENGTH + 1);
        curStart -= (Node.MAX_PREFIX_LENGTH + 1);

        while (curLength > Node.MAX_PREFIX_LENGTH) {
            InternalNode parent = new Node4(heap, key, curStart, Node.MAX_PREFIX_LENGTH, child, key[curStart + Node.MAX_PREFIX_LENGTH]);
            child = parent;
            curStart -= (Node.MAX_PREFIX_LENGTH + 1);
            curLength -= (Node.MAX_PREFIX_LENGTH + 1);
        }

        if (curLength >= 0) {
            InternalNode parent = new Node4(heap, key, start, curLength, child, key[curStart + Node.MAX_PREFIX_LENGTH]);
            child = parent;
        }
        return child;
    }

}
