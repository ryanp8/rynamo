package com.rynamo;

import com.rynamo.ring.Node;
import com.rynamo.storage.StorageLayer;

import org.rocksdb.RocksDB;

public class Main {
    public static void main(String[] args) {
        // Set up quorum values here for now
        int N = 3;
        int R = 2;
        int W = 2;
        try {
            // Create and start node
            Node node = new Node(N, R, W, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
            node.start();
        } catch (Exception e) { // TODO: Better error handling
            System.err.println(e.getMessage());
        }
    }
}