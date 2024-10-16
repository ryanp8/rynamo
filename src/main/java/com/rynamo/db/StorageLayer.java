package com.rynamo.db;

import com.google.common.primitives.Longs;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.*;


/*
* Wrapper around RocksDB
*
* Entries are stored in the form nodeID/key/version/id: value
* Each instance also stores the most recent version for each key in the form key: version
* */
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

    /*
    * Returns the version and value for the most up-to-date value associated with the key
    * */
    public Results get(String key) throws RocksDBException {
        // existence check
        long version = this.getVersion(key);
        if (version == -1) {
            return new Results(0, null);
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
        return new Results(version, results);
    }

    /*
    * Puts a key-value pair into the database. Provides the incoming version to override the old version
     * if the incoming is newer. The version parameter comes all the way from the put coordinator. So if the
     * coordinator's last known write is newer than this node's last known write, it should be updated
     *
    * */
    public long put(String key, long currentVersion, byte[] value) throws RocksDBException{
        byte[] keyBytes = key.getBytes();
        // existence check
        long myVersion = this.getVersion(key);

        if (currentVersion >= myVersion) {
            // If my version is the same as the incoming's current version, then increment it
            // If the incoming version is greater, then override my version to be that one
            myVersion = Math.max(myVersion + 1, currentVersion);
            this.db.put(keyBytes, Longs.toByteArray(myVersion));
        }

        // Give each key/version pair a random id to handle concurrent values for a version
        byte[] versionedKey = String.format("%s/%s/%d/%s", this.nodeId, key, myVersion, UUID.randomUUID())
                .getBytes(StandardCharsets.UTF_8);
        this.db.put(versionedKey, value);
        return myVersion;
    }
}
