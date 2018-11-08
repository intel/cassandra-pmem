// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.nio.ByteBuffer;
import java.util.function.*;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ArrayList;

public class ARTree {
    final Heap heap;
    private Root root;
    private int maxKeyLen;

    public ARTree(Heap heap) {
        this.heap = heap;   // maybe from TreeManager?
        this.root = new Root(heap);
    }

    @SuppressWarnings("unchecked")
    public ARTree(Heap heap, long address) {
        this.heap = heap;
        this.root = (Root)Node.rebuild(heap, address);
    }

    public long address() {
        return root.address();
    }

    //byte[] convertToByteArray(long value) {
    public static byte[] radixize(long value) {
        byte[] ret = new byte[8];
        value = value ^ 0x8000000000000000L;
        for (int i = 0; i < 8; i++) {
            ret[i] = (byte)(value >> ((7-i)*8));
        }
        return ret;
    }

    // Who should derive the token from the DecoratedKey, the ARTree or the caller?
    public void apply(byte[] radixKey, Object value, BiFunction<Object, Long, Long> merge, Transaction tx) {
        if (radixKey.length > maxKeyLen) maxKeyLen = radixKey.length;
        tx.execute(() -> {
            insert(root, root.getChild(), new SimpleLeaf(heap), radixKey, value, 0, 0, merge);
        });
    }

    @SuppressWarnings("unchecked")
    private void insert(Node parent, Node node, Leaf leaf, byte[] key, Object value, int depth, int replaceIndex, BiFunction<Object, Long, Long> merge) {
        if (node == null) {    // empty tree
            leaf.setValue(merge.apply(value, 0L));
            //else leaf.setValue(value);

            Root rt = (Root)parent;    // if tree is empty, parent is guaranteed to be root
            if (key.length > Node.MAX_PREFIX_LENGTH) {
                rt.addChild(leaf.prependNodes(key, 0, key.length));
            } else {
                rt.addChild(leaf);
                leaf.updatePrefix(key, 0, key.length);
            }
            return;
        }

        byte[] newPrefix = new byte[8];
        byte[] prefix = node.getPrefix();

        if (node.isLeaf()) {
            int matchedLength = node.checkPrefix(key, depth);
            if (matchedLength == node.getPrefixLength() && matchedLength + depth == key.length) { //replacement
                long old = ((Leaf)node).getValue();
                long newVal = merge.apply(value,old);
                if (newVal != old) ((Leaf)node).setValue(newVal);
                leaf.free();
                return;
            }
            long old = leaf.getValue();
            long newVal = merge.apply(value, old);
            if (newVal != old) leaf.setValue(newVal);
            InternalNode newNode = new Node4(this.heap);
            int i = 0;
            for (; i < (key.length-depth) && i < prefix.length && key[i+depth] == prefix[i]; i++) {
                newPrefix[i] = key[i+depth];
            }
            newNode.initPrefixLength(i);
            newNode.initPrefix(newPrefix);

            depth += i;

            node.updatePrefix(prefix, i + 1, node.getPrefixLength() - i - 1);

            int prefixLength = key.length - depth - 1;
            Node newChild = leaf;
            if (prefixLength > Node.MAX_PREFIX_LENGTH) {
                newChild = leaf.prependNodes(key, depth + 1, prefixLength);
            } else {
                leaf.updatePrefix(key, depth + 1, prefixLength);
            }

            if (depth == key.length) newNode.addBlankRadixChild(leaf);
            else newNode.addChild(key[depth], newChild);

            if (i == prefix.length) newNode.addBlankRadixChild((Leaf)node);
            else newNode.addChild(prefix[i], node);

            if (parent == root) { ((Root)parent).addChild(newNode); }
            else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
            return;
        }

        InternalNode intNode = (InternalNode)node;
        int matchedLength = intNode.checkPrefix(key, depth);
        if (matchedLength != intNode.getPrefixLength()) {
            InternalNode newNode = new Node4(this.heap);
            leaf.setValue(merge.apply(value,0L));
            int i = 0;
            for (; i < matchedLength; i++) {
                newPrefix[i] = prefix[i];
            }
            newNode.initPrefixLength(matchedLength);
            newNode.initPrefix(newPrefix);

            intNode.updatePrefix(prefix, i + 1, intNode.getPrefixLength() - i - 1);

            int prefixLength = key.length - depth - i - 1;
            Node newChild = leaf;
            if (prefixLength > Node.MAX_PREFIX_LENGTH) {
                newChild = leaf.prependNodes(key, depth + i + 1, prefixLength);
            } else {
                leaf.updatePrefix(key, depth + i + 1, prefixLength);
            }

            if (depth + i == key.length) newNode.addBlankRadixChild(leaf);
            else newNode.addChild(key[depth + i], newChild);

            newNode.addChild(prefix[i], node);

            if (parent == root) { ((Root)parent).addChild(newNode); }
            else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
            return;
        }

        depth += intNode.getPrefixLength();
        if (depth == key.length) {
            if (intNode.hasBlankRadixChild()) {
                Leaf child = intNode.findBlankRadixChild();
                long old = child.getValue();
                long newVal = merge.apply(value, old);
                if (old != newVal) child.setValue(newVal);
            }
            else{
                long old = leaf.getValue();
                long newVal = merge.apply(value, old);
                if (old != newVal) leaf.setValue(newVal);
                if (!intNode.addBlankRadixChild(leaf)) {
                    InternalNode newNode = intNode.grow();
                    ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
                    newNode.addBlankRadixChild(leaf);
                    intNode.free();
                }
            }
            leaf.free();
            // no need to update prefix for a blank radix child - it has no prefix
            return;
        }
        int childIndex = intNode.findChildIndex(key[depth]);
        Node next = intNode.getChildAtIndex(childIndex);
        if (next != null) {
            insert(node, next, leaf, key, value, depth + 1, childIndex, merge);
        } else {
            int prefixLength = key.length - depth - 1;
            leaf.setValue(merge.apply(value,0L));
            Node newChild = leaf;
            if (prefixLength > Node.MAX_PREFIX_LENGTH) {
                newChild = leaf.prependNodes(key, depth + 1, prefixLength);
            } else {
                leaf.updatePrefix(key, depth + 1, prefixLength);
            }
            if (!intNode.addChild(key[depth], newChild)) {
                InternalNode newNode = intNode.grow();
                if (parent == root) { ((Root)parent).addChild(newNode); }
                else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
                newNode.addChild(key[depth], newChild);
                intNode.free();
            }
        }
    }

    public long get(byte[] radixKey) {
        Node node;
        if ((node = root.getChild()) != null) {
            if (node.isLeaf()) {
                if ((node.getPrefixLength() == radixKey.length) && (node.checkPrefix(radixKey, 0) == radixKey.length))
                    return ((SimpleLeaf)node).getValue();
                else 
                return 0;
            } else
                return search(root.getChild(), radixKey, 0);
        }
        return 0;

    }

    @SuppressWarnings("unchecked")
    private long search(Node node, byte[] key, int depth) {
        if (node == null) {
            return 0;
        }
        if (node.isLeaf())
            return ((SimpleLeaf)node).getValue();
        int matchedLength = node.checkPrefix(key, depth);
        if (matchedLength != node.getPrefixLength()) {
            return 0;
        } else {
            depth += matchedLength;
            Node next = depth == key.length ? ((InternalNode)node).findBlankRadixChild() : ((InternalNode)node).findChild(key[depth]);
            return search(next, key, depth + 1);
        }
    }

    public void print() {
        if (root.getChild() != null)
            root.getChild().print(0);
        System.out.println("");
    }

    public Iterator getIterator() {
        return new Iterator();
    }

    public EntryIterator getEntryIterator() {
        return new EntryIterator();
    }

    class StackItem {
        NodeEntry[] entries;
        int index=0;
        int prefixLen=0;
        boolean hasBlank;

        public StackItem(NodeEntry[] entries, int prefixLen, boolean hasBlank) {
            this.entries=entries;
            this.prefixLen=prefixLen;
            this.hasBlank=hasBlank;
        }

        public int prefixLen() {
            return prefixLen;
        }

        public NodeEntry[] entries() {
            return entries;
        }

        public void saveIndex(int idx) {
            index = idx;
        }

        public int getIndex() {
            return index;
        }

        public boolean hasBlank() {
            return hasBlank;
        }

        public int length() {
            return entries.length;
        }

        public NodeEntry entryAt(int index) {
            return entries[index];
        }
    }

    public class Entry {
        byte[] key;
        long value;

        public Entry(byte[] key, long value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public long getValue() {
            return value;
        }

    }

    public class EntryIterator {
        StackItem cursor;
        int index;
        ByteBuffer keyBuf;
        Deque<StackItem> cache;
        Entry prev;
        Entry next;

        public EntryIterator() {
            cache = new ArrayDeque<>();
            Node first = root.getChild();
            if (first != null)
            {
                if (first.isLeaf()) {
                    SimpleLeaf leaf = (SimpleLeaf)first;
                    next = new Entry(first.getPrefix(), leaf.getValue());
                }
                else {
                    keyBuf = ByteBuffer.allocate(100);
                    iterate(first);
                    cursor = cache.getFirst();
                //System.out.println("constructor: Cache Size is "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                    next();
                }
            }
        }

        public boolean hasNext() {
            return (next != null);
        }

        public ARTree.Entry next() {
            prev = next;
            if (cursor == null) {
                next = null;
                return prev;
            }
            while (index >= cursor.length()) {
                if (cache.size() == 0) {
                    next = null;
                    return prev;
                }
                //System.out.println("PreCurrentFull: Cache Size is "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                try{
                keyBuf.reset().position(Math.max(0,keyBuf.position()-(1+cursor.prefixLen()))).mark();
                }catch(Exception e){
                    System.err.println("1+"+cursor.prefixLen());
                    throw new RuntimeException(e);
                }
                cache.pop();
                cursor = cache.peekFirst();
                if (cursor == null) {
                    next = null;
                    return prev;
                }
                index = cursor.getIndex()+1;
                //System.out.println("PreReset: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                keyBuf.reset();
                //System.out.println("PostCurrentFull: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
            }
            if (!cursor.entryAt(index).child.isLeaf()) {
                cursor.saveIndex(index);
                NodeEntry ne = cursor.entryAt(index);
                if (!cursor.hasBlank() || (index != 0)) keyBuf.put(ne.radix);
              //  System.out.println("PreNextNotLeaf: Cache Size is "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                iterate(cursor.entryAt(index).child);
                cursor = cache.getFirst();
             //   System.out.println("PostNextNotLeaf: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" "+keyBuf);
                index=0;
            }
            NodeEntry ne = cursor.entryAt(index++);
            SimpleLeaf leaf = (SimpleLeaf)ne.child;
            keyBuf.mark();
            if (!cursor.hasBlank() || (index != 1)) keyBuf.put(ne.radix);
            if (leaf.getPrefixLength() > 0) keyBuf.put(leaf.getPrefix());
            next = new Entry(Arrays.copyOf(keyBuf.array(),keyBuf.position()),leaf.getValue());
            keyBuf.reset();
            return prev;
        }

        void iterateChildren(InternalNode current) {
            NodeEntry[] entries = current.getEntries();
            cache.push(new StackItem(entries, current.getPrefixLength(),current.hasBlankRadixChild()));
            if (current.getPrefixLength() > 0) keyBuf.put(current.getPrefix());
            if (!current.hasBlankRadixChild() && !entries[0].child.isLeaf()) keyBuf.put(entries[0].radix);
            //System.out.println("partial key: "+new String(Arrays.copyOf(keyBuf.array(),keyBuf.position()))+" "+keyBuf);
            iterate(entries[0].child);
        }

        void iterate(Node current) {
            if (!current.isLeaf()) {
                iterateChildren((InternalNode)current);
            } else {
                return;
            }
        }


    }


    public class Iterator {
        StackItem cursor;
        int index;
        Deque<StackItem> cache;
        long next;
        long prev;

        public Iterator() {
            cache = new ArrayDeque<>();
            Node first = root.getChild();
            if (first != null)
            {
                if (first.isLeaf()) {
                    SimpleLeaf leaf = (SimpleLeaf)first;
                    next = leaf.getValue();
                }
                else {
                    iterate(first);
                    cursor = cache.getFirst();
                    next();
                    //System.out.println("Cache Size is "+cache.size()+"; cursor size is "+cursor.length+" index is "+index);
                }
            }
        }

        public boolean hasNext() {
            return (next != 0);
        }

        public long next() {
            prev = next;
            if (cursor == null) {
                next = 0;
                return prev;
            }
            while (index >= cursor.length()) {
                if (cache.size() == 0) {
                    next = 0;
                    return prev;
                }
                //System.out.println("PreCurrentFull: Cache Size is "+cache.size()+"; cursor size is "+cursor.length+" index is "+index);
                cache.pop();
                cursor = cache.peekFirst();
                if (cursor == null) {
                    next = 0;
                    return prev;
                }
                index = cursor.getIndex()+1;
                //System.out.println("PostCurrentFull: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length+" index is "+index);
            }
            if (!cursor.entryAt(index).child.isLeaf()) {
                cursor.saveIndex(index);
                //System.out.println("PreNextNotLeaf: Cache Size is "+cache.size()+"; cursor size is "+cursor.length+" index is "+index);
                iterate(cursor.entryAt(index).child);
                cursor = cache.getFirst();
                //System.out.println("PostNextNotLeaf: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length);
                index=0;
            }
            next = ((SimpleLeaf)cursor.entryAt(index++).child).getValue();
            return prev;
        }

        void iterateChildren(InternalNode current) {
            NodeEntry[] entries = current.getEntries();
            cache.push(new StackItem(entries, current.getPrefixLength(),current.hasBlankRadixChild()));
            if (entries != null) {
                iterate(entries[0].child);
                }
        }

        void iterate(Node current) {
            if (!current.isLeaf()) {
                iterateChildren((InternalNode)current);
            } else {
                return;
            }
        }


    }

}
