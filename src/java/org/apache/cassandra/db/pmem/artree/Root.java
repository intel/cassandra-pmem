/*
 * Copyright (C) 2018-2020 Intel Corporation
 *
 * SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause
 *
 */

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.util.function.Consumer;

public final class Root extends Node {
    private static final long SIZE = Node.HEADER_SIZE + 8;  // 8 byte pointer to first node in tree
    private static final long CHILD_OFFSET = Node.HEADER_SIZE;

    Root(TransactionalHeap heap) {
        super(heap, SIZE);
        initType(Node.ROOT_TYPE);
    }

    Root(TransactionalHeap heap, TransactionalUnboundedMemoryBlock mb) {
        super(heap, mb);
    }

    boolean addChild(Node node) {
        mb.setLong(CHILD_OFFSET, node.address());
        return true;
    }

    Node getChild() {
        return Node.rebuild(heap, mb.getLong(CHILD_OFFSET));
    }

    boolean isLeaf() { return false; }

    @Override
    void destroy(Consumer<Long> cleaner) {
        Node child = getChild();
        if (child != null) {
            child.destroy(cleaner);
            child.free();
            mb.setLong(CHILD_OFFSET, 0L);
        }
    }
    
    void deleteChild() {
        destroy(null);    
    }
}
