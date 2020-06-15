/*
 * Copyright (C) 2018-2020 Intel Corporation
 *
 * SPDX-License-Identifier: Apache-2.0 OR BSD-3-Clause
 *
 */

package org.apache.cassandra.db.pmem.artree;

public class NodeEntry{
        byte radix;
        Node child;

        public NodeEntry(byte radix, Node child) {
            this.radix = radix;
            this.child = child;
        }
        
    }
