// TODO: COPYRIGHT HEADER

package org.apache.cassandra.db.pmem.artree;

import lib.llpl.*;
import java.nio.ByteBuffer;
import java.util.function.*;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Optional;

public class ARTree {
    final TransactionalHeap heap;
    private Root root;
    private int maxKeyLen;

    public ARTree(TransactionalHeap heap) {
        this.heap = heap;   // maybe from TreeManager?
        this.root = new Root(heap);
    }

    public static void registerAllocationClasses(TransactionalHeap heap) {
        heap.registerAllocationSize(SimpleLeaf.SIZE, true);
        heap.registerAllocationSize(Node4.SIZE, true);
        heap.registerAllocationSize(Node16.SIZE, true);
        heap.registerAllocationSize(Node48.SIZE, true);
        heap.registerAllocationSize(Node256.SIZE, true);
    }

    @SuppressWarnings("unchecked")
    public ARTree(TransactionalHeap heap, long address) {
        this.heap = heap;
        this.root = (Root)Node.rebuild(heap, address);
    }

    public long address() {
        return root.address();
    }

    public enum Operation {
        DELETE_NODE,
        END,
        NO_OP;
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
        tx.run(() -> {
            //insert(root, root.getChild(), new SimpleLeaf(heap), radixKey, value, 0, 0, merge);
            insert(root, root.getChild(), radixKey, value, 0, 0, merge);
        });
    }

    @SuppressWarnings("unchecked")
    //private void insert(Node parent, Node node, Leaf leaf, byte[] key, Object value, int depth, int replaceIndex, BiFunction<Object, Long, Long> merge) {
    private void insert(Node parent, Node node, byte[] key, Object value, int depth, int replaceIndex, BiFunction<Object, Long, Long> merge) {
        if (node == null) {    // empty tree
            //leaf.setValue(merge.apply(value, 0L));
            long leafValue = merge.apply(value, 0L);
            //else leaf.setValue(value);

            Root rt = (Root)parent;    // if tree is empty, parent is guaranteed to be root
            Node leaf = SimpleLeaf.create(this.heap, key, 0, key.length, leafValue);
            /*if (key.length > Node.MAX_PREFIX_LENGTH) {
                rt.addChild(leaf.prependNodes(key, 0, key.length));
            } else {
                rt.addChild(leaf);
                leaf.updatePrefix(key, 0, key.length);
            }*/
            rt.addChild(leaf);
            return;
        }

        byte[] newPrefix = new byte[8];
        byte[] prefix = node.getPrefix();

        if (node.isLeaf()) {
            int matchedLength = node.checkPrefix(key, depth);
            if (matchedLength == node.getPrefixLength() && matchedLength + depth == key.length) {
                //replacement
                long old = ((Leaf)node).getValue();
                long newVal = merge.apply(value,old);
                if (newVal != old) ((Leaf)node).setValue(newVal);
                return;
            }
            long newVal = merge.apply(value, 0L);
            InternalNode newNode;
            int i = 0;
            for (; i < (key.length-depth) && i < prefix.length && key[i+depth] == prefix[i]; i++) {
                newPrefix[i] = key[i+depth];
            }

            depth += i;

            node.updatePrefix(prefix, i + 1, node.getPrefixLength() - i - 1);

            int prefixLength = key.length - depth - 1;
            Node newChild = SimpleLeaf.create(this.heap, key, depth + 1, prefixLength, newVal);

            if (depth == key.length) newNode = new Node4(this.heap, newPrefix, i, true, newChild, (byte)0, node, prefix[i]);
            else if (i == prefix.length) newNode = new Node4(this.heap, newPrefix, i, true, (Leaf)node, (byte)0, newChild, key[depth]);
            else newNode = new Node4(this.heap, newPrefix, i, false, newChild, key[depth], node, prefix[i]);

            if (parent == root) { ((Root)parent).addChild(newNode); }
            else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
            return;
        }

        InternalNode intNode = (InternalNode)node;
        int matchedLength = intNode.checkPrefix(key, depth);
        if (matchedLength != intNode.getPrefixLength()) {
            InternalNode newNode;
            long leafVal = merge.apply(value, 0L);
            int i = 0;
            for (; i < matchedLength; i++) {
                newPrefix[i] = prefix[i];
            }

            intNode.updatePrefix(prefix, i + 1, intNode.getPrefixLength() - i - 1);

            int prefixLength = key.length - depth - i - 1;
            Node newChild = SimpleLeaf.create(this.heap, key, depth + i + 1, prefixLength, leafVal);

            if (depth + i == key.length) newNode = new Node4(this.heap, newPrefix, matchedLength, true, newChild, (byte)0, node, prefix[i]);
            else newNode = new Node4(this.heap, newPrefix, matchedLength, false, newChild, key[depth + i], node, prefix[i]);

            if (parent == root) { ((Root)parent).addChild(newNode); }
            else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
            return;
        }

        depth += intNode.getPrefixLength();
        if (depth == key.length) {
            //this insertion will be a blankradix child to this internal node
            if (intNode.hasBlankRadixChild()) {
                Leaf child = intNode.findBlankRadixChild();
                long old = child.getValue();
                long newVal = merge.apply(value, old);
                if (old != newVal) child.setValue(newVal);
            }
            else{
                long newVal = merge.apply(value, 0L);
                SimpleLeaf leaf = new SimpleLeaf(this.heap, newVal);
                if (!intNode.addBlankRadixChild(leaf)) {
                    InternalNode newNode = intNode.grow(leaf, Optional.empty());
                    ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
                    intNode.free();
                }
            }
            // no need to update prefix for a blank radix child - it has no prefix
            return;
        }
        //descending find next node with matching radix
        int childIndex = intNode.findChildIndex(key[depth]);
        Node next = intNode.getChildAtIndex(childIndex);
        if (next != null) {
            insert(node, next, key, value, depth + 1, childIndex, merge);
        } else {
            // found insertion point. insert leaf
            int prefixLength = key.length - depth - 1;
            long leafVal = merge.apply(value, 0L);
            Node newChild = SimpleLeaf.create(this.heap, key, depth + 1, prefixLength, leafVal);
            if (!intNode.addChild(key[depth], newChild)) {
                InternalNode newNode = intNode.grow(newChild, Optional.of(key[depth]));
                if (parent == root) { ((Root)parent).addChild(newNode); }
                else ((InternalNode)parent).putChildAtIndex(replaceIndex, newNode);
                intNode.free();
            }
        }
    }

    public long get(byte[] radixKey) {
        Node node;
        if ((node = root.getChild()) != null) {
            /*if (node.isLeaf()) {
                if ((node.getPrefixLength() == radixKey.length) && (node.checkPrefix(radixKey, 0) == radixKey.length))
                    return ((SimpleLeaf)node).getValue();
                else
                return 0;
            } else*/
                return search(root.getChild(), radixKey, 0, null, null);
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private long search(Node node, byte[] key, int depth, SearchHelper helper, Consumer<Long> c) {
        if (node == null) {
            return 0;
        }
        Node next;
        int matchedLength = node.checkPrefix(key, depth);
        if (matchedLength != node.getPrefixLength()) {
            return 0;
        }
        if (node.isLeaf())
            return ((SimpleLeaf)node).getValue();
        else {
            depth += matchedLength;
            boolean blank = (depth == key.length);
            next = blank ? ((InternalNode)node).findBlankRadixChild() : ((InternalNode)node).findChild(key[depth]);
            long l = search(next, key, depth + 1, helper, c);
            //if (l != 0 && helper != null) {
            if (helper != null) {
                helper.apply(node, next, (blank ? null : key[Math.min(depth,key.length-1)]), c);
            }
            return l;
        }
    }

    public void print() {
        if (root.getChild() != null)
            root.getChild().print(0);
        System.out.println("");
    }

    public void clear(Consumer<Long> cleaner) {
        root.destroy(cleaner);
    }

    @FunctionalInterface
    public interface SearchHelper {
        void apply(Node parent, Node child, Byte radix, Consumer<Long> cleaner);
    }

    void deleteNodes(Node parent, Node child, Byte radix, Consumer<Long> cleaner) {
        Transaction.run(heap, ()->{
            if (child.isLeaf()) {
            //    System.out.println("Ascending: deleting node at radix "+new String(new byte[]{radix}));
                if (cleaner != null) cleaner.accept(((SimpleLeaf)child).getValue());
                child.free();
                ((InternalNode)parent).deleteChild(radix);
            }
            else if (((InternalNode)child).getChildrenCount() == 0) {
            //    System.out.println("Ascending: deleting node at radix "+new String(new byte[]{radix}));
                child.free();
                ((InternalNode)parent).deleteChild(radix);
            }
        });
    }

    public void delete(byte[] key, Consumer<Long> cleaner) {
            search(root.getChild(), key, 0, this::deleteNodes , cleaner);
    }

    public void forEach(BiFunction<byte[], Long, Operation> fcn) {
        System.out.println("vacuuming...");
        new InternalIterator(fcn, new byte[0]);
    }

    public void forEach(BiFunction<byte[], Long, Operation> fcn, byte[] firstKey) {
        System.out.println("vacuuming...");
        new InternalIterator(fcn, firstKey);
    }

    public Iterator getIterator() {
        return new Iterator();
    }

    String getString(byte[] key) {
        StringBuffer sb = new StringBuffer("[ ");
        for (int i = 0; i < key.length; i++) {
            sb.append(key[i] + " ");
        }
        sb.append("]");
        return sb.toString();
    }

    public EntryIterator getEntryIterator() {
        return new EntryIterator();
    }

    public EntryIterator getEntryIterator(byte[] firstKey) {
        return new EntryIterator(firstKey, true, null, false);
    }

    public EntryIterator getEntryIterator(byte[] firstKey, byte[] lastKey) {
        return new EntryIterator(firstKey, true, lastKey, false);
    }

    public EntryIterator getEntryIterator(byte[] firstKey, boolean firstInclusive) {
        return new EntryIterator(firstKey, firstInclusive, null, false);
    }

    public EntryIterator getEntryIterator(byte[] firstKey, boolean firstInclusive, byte[] lastKey, boolean lastInclusive) {
        return new EntryIterator(firstKey, firstInclusive, lastKey, lastInclusive);
    }

    class StackItem {
        NodeEntry[] entries;
        int index = 0;
        int prefixLen = 0;
        private final boolean hasBlank;

        public StackItem(NodeEntry[] entries, int prefixLen, boolean hasBlank) {
            this.entries = entries;
            this.prefixLen = prefixLen;
            this.hasBlank = hasBlank;
        }

        public StackItem(NodeEntry[] entries, int prefixLen, boolean hasBlank, Byte radix) {
            this.entries=entries;
            this.prefixLen=prefixLen;
            this.hasBlank=hasBlank;
            if (!hasBlank && radix != entries[0].radix) calcIndex(radix);
        }

        void calcIndex(byte radix) {
            for (int i = 1; i < entries.length; i++) {
                if (entries[i].radix == radix) {
                    index = i;
                    break;
                }
            }
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

    private class InternalIterator {
        byte[] key;
        byte[] firstKey;
        boolean visited = false;
        boolean found = true;

        BiFunction<byte[], Long, Operation> fcn;

        public InternalIterator(BiFunction<byte[], Long, Operation> fcn, byte[] firstKey) {
            this.fcn = fcn;
            this.key = new byte[50];
            this.firstKey = firstKey;
            if (firstKey.length > 0) {
                 System.arraycopy(firstKey, 0, key, 0, firstKey.length);
                 found = false;
            }
            Node node = root.getChild();
            findLowestKey(node, 0);
            if (!node.isLeaf() && ((InternalNode)node).getChildrenCount() == 0) {
                root.deleteChild();
            }
        }

        void findLowestKey(Node node, int depth) {
            // System.out.println("FLK: depth is "+depth);
            boolean blank = false;
            visited = false;
            if (node == null) return;
            if (node.isLeaf()) {
                 // System.out.println("node is leaf! value->"+((SimpleLeaf)node).getValue());
                 // System.out.println("next Key is "+ (new String(key, 0, depth))+" depth is "+depth);
                //if (!found) found = true;
                return;
            }
            while(true) {
                Node next = null;
                // copy node prefix into key
                byte[] prefix = node.getPrefix();
                if (!found) {
                    int matchedLength = node.checkPrefix(key, depth);
                    if (matchedLength != node.getPrefixLength()) {
                        found = true;
                        // System.out.println("not a match!");
                        break;
                    }
                    else {
                        depth += matchedLength;
                        next = (depth == firstKey.length) ? ((InternalNode)node).findBlankRadixChild() : ((InternalNode)node).findChild(key[depth++]);
                    }
                } else {
                if (prefix.length != 0) {
                    System.arraycopy(prefix, 0, key, depth, prefix.length);
                    depth+=prefix.length;
                }
                Byte b;
                // blankRadix check
                if (!blank && !visited && ((InternalNode)node).hasBlankRadixChild()) {
                    blank = true;
                } else {
                    blank = false;
                    b = ((InternalNode)node).findLowestRadix(key[depth], visited);
                    if (b == null) break;
                    key[depth] = b;
                }
                // System.out.println("blank is "+blank);
                next = blank ? ((InternalNode)node).findBlankRadixChild() : ((InternalNode)node).findChild(key[depth++]);
                // System.out.println("Descending: Parent is "+node.address()+" Child is "+next.address()+" radix is "+new String(new byte[]{key[depth-1]})+" depth is "+depth);
                }
                // System.out.println("Descending: Parent is "+node.address()+" Child is "+next.address()+" radix is "+new String(new byte[]{key[depth-1]})+" depth is "+depth);
                if (depth == 0 || next == null) break;
                findLowestKey(next, depth);
                // Ascending
                if (!blank) visited = true;
                // System.out.println("Ascending: Parent is "+node.address()+" Child is "+((next == null) ? "null" : next.address())+" partial key is "+new String(Arrays.copyOf(key, depth))+" depth is "+depth);
                if (next.isLeaf()) {
                    byte[] leafPrefix = next.getPrefix();
                    if (!found) {
                        found = true;
                        // System.out.println("leafprefixLen is "+leafPrefix.length);
                        // System.out.println("printing leafprefix ...");
                        for (int i=0; i<leafPrefix.length; i++) {
                            System.out.println(Long.toHexString(leafPrefix[i]));
                        }
                        // System.out.println("depth is "+depth);
                        // System.out.println("printing key ...");
                        /*for (int i=depth; i<(depth + leafPrefix.length); i++) {
                             System.out.println(Long.toHexString(key[i]));
                        }*/
                        int matchedLength = next.checkPrefix(key, depth);
                        if (matchedLength != leafPrefix.length) {
                            // System.out.println("leaf prefix was not a match! matchedlen is "+matchedLength);
                            //if (firstKey.length <= depth || Bytes.compare
                            depth-=prefix.length; if (!blank) depth--;
                            continue;
                        }
                    }
                    if (leafPrefix.length != 0) {
                        System.arraycopy(leafPrefix, 0, key, depth, leafPrefix.length);
                    }
                    final int d = depth + leafPrefix.length;
                    Operation op = fcn.apply(Arrays.copyOf(key, d), ((SimpleLeaf)next).getValue());
                    if (op == Operation.DELETE_NODE) {
                        final Node fnext = next;
                        final boolean fblank = blank;
                        Transaction.run(heap, ()-> {
                            // System.out.println("deleting child, addr: "+fnext.address()+" with radix "+new String(new byte[]{key[d - leafPrefix.length - 1]}));
                             fnext.free();
                            if (fblank) ((InternalNode)node).deleteChild(null);
                            else {((InternalNode)node).deleteChild(key[d - leafPrefix.length - 1]);}
                        });
                    }
                    else if (op == Operation.END) break;
                } else if (((InternalNode)next).getChildrenCount() == 0) {
                    // System.out.println("internal node: "+next.address()+" has no children so  deleting. parent: "+node.address()+" radix is "+new String(new byte[]{key[depth - 1]}));
                    final int d = depth;
                    final Node fnext = next;
                    Transaction.run(heap, ()-> {
                        fnext.free();
                        ((InternalNode)node).deleteChild(key[d - 1]);
                    });
                }
                // remove prefix from node
                depth-=prefix.length;
                if (!blank)  depth--;
            }
        }
    }

    public class EntryIterator {
        StackItem cursor;
        int index;
        byte[] lastKey = null;
        boolean lastInclusive = false;
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

        void buildCache(Node parent, Node child, Byte radix, Consumer<Long> cleaner) {
            StackItem item;
            if (radix == null) item = new StackItem(((InternalNode)parent).getEntries(), parent.getPrefixLength(), true);
             else item = new StackItem(((InternalNode)parent).getEntries(), parent.getPrefixLength(), radix == null, radix);
            cache.addLast(item);
            byte[] ba = parent.getPrefix();
            if (ba.length > 0) keyBuf.put(ba);
            if (radix != null) keyBuf.put(radix);
            else keyBuf.position(keyBuf.position() + 1);
        }

        public EntryIterator(byte[] firstKey, boolean firstInclusive, byte[] lastKey, boolean lastInclusive) {
            cache = new ArrayDeque<>();
            Node first = root.getChild();
            this.lastKey = lastKey;
            this.lastInclusive = lastInclusive;
            if (first != null)
            {
                if (first.isLeaf()) {
                    SimpleLeaf leaf = (SimpleLeaf)first;
                    next = (Arrays.equals(firstKey, first.getPrefix())) ? new Entry(first.getPrefix(), leaf.getValue()) : null;
                }
                else if (firstKey == null) {
                    keyBuf = ByteBuffer.allocate(100);
                    iterate(first);
                    cursor = cache.getFirst();
                    next();
                }
                else {
                    keyBuf = ByteBuffer.allocate(100);
                    search(first, firstKey, 0, this::buildCache, null);
                    int pos = keyBuf.position(); keyBuf.position(0);
                    if (pos > 0) keyBuf.put(firstKey, 0, pos - 1);
                    cursor = cache.getFirst();
                    index = cursor.getIndex();
                    next();
                    if (!firstInclusive && Arrays.equals(firstKey, next.getKey())) next();
                }
                prev = next;
            }
        }

        public boolean hasNext() {
            //return (next != null && !Arrays.equals(lastKey,next.getKey()));
            return lastInclusive ? (next != null && prev != null && !Arrays.equals(lastKey,prev.getKey())) : (next != null && !Arrays.equals(lastKey, next.getKey()));
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
                // System.out.println("PreCurrentFull: Cache Size is "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                //try{
                keyBuf.reset().position(Math.max(0, keyBuf.position() - (1 + cursor.prefixLen()))).mark();
                /*}catch(Exception e){
                    System.err.println("1+"+cursor.prefixLen());
                    throw new RuntimeException(e);
                }*/
                cache.pop();
                cursor = cache.peekFirst();
                if (cursor == null) {
                    next = null;
                    return prev;
                }
                index = cursor.getIndex() + 1;
                // System.out.println("PreReset: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                keyBuf.reset();
                // System.out.println("PostCurrentFull: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
            }
            if (!cursor.entryAt(index).child.isLeaf()) {
                cursor.saveIndex(index);
                NodeEntry ne = cursor.entryAt(index);
                if (!cursor.hasBlank() || (index != 0)) keyBuf.put(ne.radix);
                // System.out.println("PreNextNotLeaf: Cache Size is "+cache.size()+"; cursor size is "+cursor.length()+" index is "+index+" "+keyBuf);
                iterate(cursor.entryAt(index).child);
                cursor = cache.getFirst();
                // System.out.println("PostNextNotLeaf: Cache Size is now "+cache.size()+"; cursor size is "+cursor.length()+" "+keyBuf);
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
            boolean blank;
            cache.push(new StackItem(entries, current.getPrefixLength(), blank = current.hasBlankRadixChild()));
            if (current.getPrefixLength() > 0) keyBuf.put(current.getPrefix());
            if (!blank && !entries[0].child.isLeaf()) keyBuf.put(entries[0].radix);
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
