package com.rynamo.db;

import com.google.common.primitives.Longs;
import com.rynamo.ring.Node;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class StorageLayer {

    private final RocksDB db;
    private final String nodeId;
    public StorageLayer(String nodeId) throws org.rocksdb.RocksDBException {
        this.nodeId = nodeId;
        Options options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options,String.format("./tmp/%s", nodeId));
    }

    public long getVersion(String key) {
        byte[] keyBytes = key.getBytes();
        try {
            byte[] versionBytes = this.db.get(keyBytes);
            if (versionBytes == null) {
                return 0;
            }
            return Longs.fromByteArray(versionBytes);
        } catch (RocksDBException e) {
            return -1;
        }
    }

    public void setVersion(String key, long version) {
        byte[] keyBytes = key.getBytes();
        try {
            this.db.put(keyBytes, Longs.toByteArray(version));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
    public Row get(String key) throws RocksDBException {
        byte[] keyBytes = key.getBytes();
        // existence check
        long version = this.getVersion(key);
        if (version == -1) {
            return new Row(0, null);
        }
        String prefix = String.format("%s/%s/%d", this.nodeId, key, version); // key/version

        // Get value for all keys with prefix because there may be concurrent values for each version
        List<byte[]> results = new ArrayList<>();
        RocksIterator iterator = this.db.newIterator();
        for (iterator.seek(prefix.getBytes()); iterator.isValid(); iterator.next()) {
            String foundKey = new String(iterator.key());
            if (!foundKey.startsWith(prefix)) break;
            results.add(iterator.value());
        }
        return new Row(version, results);
    }

    public long put(String key, long previousVersion, byte[] value) throws RocksDBException{
        byte[] keyBytes = key.getBytes();
        // existence check
        long currentVersion = this.getVersion(key);

        if (previousVersion >= currentVersion) {
            currentVersion = Math.max(currentVersion + 1, previousVersion);
            this.db.put(keyBytes, Longs.toByteArray(currentVersion));
        }

        // Give each key/version pair a random id to handle concurrent values for a version
        byte[] versionedKey = String.format("%s/%s/%d/%s", this.nodeId, key, currentVersion, UUID.randomUUID())
                .getBytes(StandardCharsets.UTF_8);
        this.db.put(versionedKey, value);
        return currentVersion;
    }
}
