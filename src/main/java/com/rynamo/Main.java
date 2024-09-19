package com.rynamo;

import com.rynamo.db.StorageLayer;
import com.rynamo.ring.Node;
import org.rocksdb.RocksDB;

public class Main {
    public static void main(String[] args) {
        int N = 3;
        int R = 2;
        int W = 2;
        try {
            Node node = new Node(N, R, W, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
            node.start();
//            StorageLayer db = new StorageLayer(node.getId());
//            long version = db.put("a", 0, new byte[]{1});
//            System.out.println(version);
//            System.out.println(db.get("a"));
//
//            version = db.put("a", 0, new byte[]{2});
//            System.out.println(version);
//            System.out.println(db.get("a"));
//
//            version = db.put("a", 1, new byte[]{3});
//            System.out.println(version);
//            System.out.println(db.get("a"));
//
//            version = db.put("a", 0, new byte[]{4});
//            System.out.println(version);
//            System.out.println(db.get("a"));
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}