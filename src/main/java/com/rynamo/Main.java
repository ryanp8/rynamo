package com.rynamo;

import com.rynamo.ring.Node;

public class Main {
    public static void main(String[] args) {
        int N = 3;
        int R = 2;
        int W = 2;
        try {
            Node node = new Node(N, R, W, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            node.start();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}