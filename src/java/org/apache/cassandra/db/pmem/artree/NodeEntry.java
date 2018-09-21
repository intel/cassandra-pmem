package org.apache.cassandra.db.pmem.artree;

public class NodeEntry{
        byte radix;
        Node child;

        public NodeEntry(byte radix, Node child) {
            this.radix = radix;
            this.child = child;
        }
        
    }
