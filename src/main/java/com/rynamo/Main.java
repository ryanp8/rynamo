package com.rynamo;

import com.rynamo.db.DBClient;
import com.rynamo.ring.Node;
import org.rocksdb.RocksDB;

public class Main {
    public static void main(String[] args) {
        try {
            Node node = new Node(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            node.startRPCServer();
            node.startMembershipGossip();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}