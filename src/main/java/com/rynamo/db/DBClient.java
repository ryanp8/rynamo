package com.rynamo.db;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DBClient {

    private final RocksDB db;
    public DBClient(String name) throws org.rocksdb.RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options,String.format("./tmp/%s", name));
    }

    public byte[] get(String key) throws RocksDBException {
        byte[] keyBytes = key.getBytes();
        return this.db.get(keyBytes);
    }

    public void put(String key, byte[] value) throws RocksDBException{
        byte[] keyBytes = key.getBytes();
        this.db.put(keyBytes, value);
    }
}
