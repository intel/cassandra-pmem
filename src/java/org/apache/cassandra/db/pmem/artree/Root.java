// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;

public final class Root extends Node {
    private static final long SIZE = Node.HEADER_SIZE + 8;  // 8 byte pointer to first node in tree
    private static final long CHILD_OFFSET = Node.HEADER_SIZE;

    Root(Heap heap) {
        super(heap, SIZE);
        initType(Node.ROOT_TYPE);
    }

    Root(Heap heap, MemoryBlock<Unbounded> mb) {
        super(heap, mb);
    }

    boolean addChild(Node node) {
        mb.setTransactionalLong(CHILD_OFFSET, node.address());
        return true;
    }

    Node getChild() {
        return Node.rebuild(heap, mb.getLong(CHILD_OFFSET));
    }

    boolean isLeaf() { return false; }
}
