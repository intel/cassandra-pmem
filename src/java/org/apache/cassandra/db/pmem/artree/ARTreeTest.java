package org.apache.cassandra.db.pmem.artree;
import lib.llpl.*;
import java.util.function.BiFunction;

public class ARTreeTest {
    public static void main(String[] args) {
        Heap heap = Heap.getHeap("/mnt/mem/persistent_heap", 2000000000L);
        ARTree tree = new ARTree(heap);

        BiFunction<Object, Long, Long> merge = (l1, l2) -> {
            if (l2 == null) return (Long)l1;
            else return l2;
        };

        for (int i = (int)'0'; i < (int)'0' + 60; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("ABC");
            sb.append((char)i);
            String str = sb.toString();
            tree.apply(str.getBytes(), 0x1234L << 8 | (byte)i, merge, new Transaction(heap));
            System.out.println("Just inserted key " + str + ", index " + (i - (int)'0'));
            if (i - (int)'0' == 3) tree.print();
            if (i - (int)'0' == 15) tree.print();
            if (i - (int)'0' == 47) tree.print();
        }
        tree.print();

        System.out.println("********** Value Iterator **********");
        ARTree.Iterator it1 = tree.getIterator();
        long v;
        while (it1.hasNext()) {
            v = it1.next();
            System.out.println("Value: "+v);
        }

        System.out.println("********** Entry Iterator **********");

        ARTree.EntryIterator it2 = tree.getEntryIterator();
        ARTree.Entry e;
        while (it2.hasNext()) {
            e = it2.next();
            System.out.println("Key: "+e.getKey()+" -> Value: "+e.getValue());
        }


        /*tree.apply(0x1122334400000000L, 0x1122334400000000L, new Transaction());
        tree.apply(0x1122336600000000L, 0x1122336600000000L, new Transaction());
        tree.apply(0x1122334455000000L, 0x1122334455000000L, new Transaction());
        tree.apply(0x1122334466000000L, 0x1122334466000000L, new Transaction());
        tree.apply(0x1122334477000000L, 0x1122334477000000L, new Transaction());
        tree.apply(0x1122334488000000L, 0x1122334488000000L, new Transaction());
        tree.print();

        System.out.println(Long.toHexString(tree.get(0x1122334400000000L)));
        System.out.println(Long.toHexString(tree.get(0x1122336600000000L)));
        System.out.println(Long.toHexString(tree.get(0x1122334455000000L)));
        System.out.println(Long.toHexString(tree.get(0x1122334466000000L)));
        System.out.println(Long.toHexString(tree.get(0x1122334477000000L)));
        System.out.println(Long.toHexString(tree.get(0x1122334488000000L)));

        String s1 = "TEACHER";
        byte[] b1 = s1.getBytes();
        tree.apply(b1, 0x12345L, 0x12345L, new Transaction());

        String s2 = "TEACH";
        byte[] b2 = s2.getBytes();
        tree.apply(b2, 0x1234L, 0x1234L, new Transaction());

        String s3 = "TEAPOT";
        byte[] b3 = s3.getBytes();
        tree.apply(b3, 0x123456L, 0x123456L, new Transaction());
        tree.print();

        String s4 = "TEAPOT";
        byte[] b4 = s4.getBytes();
        tree.apply(b4, 0x123456L, 0x1234567L, new Transaction());
        tree.print();

        String s5 = "TEA";
        byte[] b5 = s5.getBytes();
        tree.apply(b5, 0x123456L, 0x123456L, new Transaction());

        String s6 = "TEAABCDEFGHIJKLMNOPQRST";
        byte[] b6 = s6.getBytes();
        tree.apply(b6, 0x12345678L, 0x12345678L, new Transaction());

        tree.print();*/
    }
}
