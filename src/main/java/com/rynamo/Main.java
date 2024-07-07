package com.rynamo;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Node node = new Node(args[0], Integer.parseInt(args[1]));
        node.startRPCServer();
        node.startMembershipGossip();
    }
}