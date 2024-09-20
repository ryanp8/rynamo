package com.rynamo.ring.coordinate;

import com.google.protobuf.ByteString;
import com.rynamo.db.Row;
import com.rynamo.grpc.storage.GetResponse;
import com.rynamo.grpc.storage.PutResponse;
import com.rynamo.grpc.storage.StorageGrpc;
import com.rynamo.ring.Node;
import com.rynamo.ring.entry.ActiveEntry;
import com.rynamo.ring.entry.RingEntry;
import io.grpc.StatusRuntimeException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.ForkJoinTask.invokeAll;

public class Coordinator {
    private final Node node;
    public Coordinator(Node node) {
        this.node = node;
    }

    public CoordinateResponse coordinateGet(String key) {
        System.out.println(key);
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        List<byte[]> results = new ArrayList<>();
        List<Callable<List<ByteString>>> tasks = calculateGetTasks(key, preferenceList);

        int cores = Runtime.getRuntime().availableProcessors();
        int reads = 0;
        ExecutorService taskExecutor = Executors.newFixedThreadPool(cores);
        try {
            List<Future<List<ByteString>>> taskResults = taskExecutor.invokeAll(tasks);
            for (Future<List<ByteString>> future : taskResults) {
                try {
                    List<ByteString> oneTaskResults = future.get();
                    if (!oneTaskResults.isEmpty()) {
                        for (ByteString value : oneTaskResults) {
                            results.add(value.toByteArray());
                        }
                        reads++;
                    }
                } catch (CancellationException | ExecutionException ce) {
                    ce.printStackTrace();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
            }
            return new CoordinateResponse(reads, 0, results);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new CoordinateResponse(reads, 0, results);
        }
    }

    private List<Callable<List<ByteString>>> calculateGetTasks(String key, List<RingEntry> preferenceList) {
        List<Callable<List<ByteString>>> tasks = new ArrayList<>();
        int activeNodesTried = 0;
        for (int i = 0; i < preferenceList.size() && activeNodesTried < this.node.N; i++) {
            RingEntry entry = preferenceList.get(i);
            if (entry instanceof ActiveEntry active) {
                activeNodesTried++;
                tasks.add(() -> {
                    try {
                        GetResponse response = active.get(key);
                        return response.getValueList();
                    } catch (StatusRuntimeException e) {
                        e.printStackTrace();
                        return new ArrayList<>() {};
                    }
                });
            }
        }
        return tasks;
    }

    public CoordinateResponse coordinatePut(String key, long version, byte[] val) {
        List<RingEntry> preferenceList = this.node.getPreferenceList(key);
        List<Callable<Long>> tasks = this.calculatePutTasks(key, version, val, preferenceList);

        int cores = Runtime.getRuntime().availableProcessors();
        int writes = 0;
        ExecutorService taskExecutor = Executors.newFixedThreadPool(cores);

        try {
            List<Future<Long>> taskResults = taskExecutor.invokeAll(tasks);
            for (Future<Long> future : taskResults) {
                try {
                    Long oneTaskResults = future.get();
                    if (oneTaskResults > -1) {
                        writes++;
                    }
                } catch (CancellationException | ExecutionException ce) {
                    ce.printStackTrace();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt(); // ignore/reset
                }
            }
            if (writes>= this.node.W) {
                this.node.db().setVersion(key, version + 1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new CoordinateResponse(0, writes, null);
    }

    private List<Callable<Long>> calculatePutTasks(String key, long version, byte[] val,
                                                               List<RingEntry> preferenceList) {
        List<Callable<Long>> tasks = new ArrayList<>();
        int activeNodesTried = 0;
        for (int i = 0; i < preferenceList.size() && activeNodesTried < this.node.N; i++) {
            RingEntry entry = preferenceList.get(i);
            if (entry instanceof ActiveEntry active) {
                activeNodesTried++;
                tasks.add(() -> {
                    try {
                        PutResponse response = active.put(key, version, val);
                        return response.getVersion();
                    } catch (StatusRuntimeException e) {
                        e.printStackTrace();
                        return (long) -1;
                    }
                });
            }
        }
        return tasks;
    }
}
