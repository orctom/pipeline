package com.orctom.pipeline.utils;

import com.orctom.pipeline.Configurator;
import com.orctom.pipeline.exception.MessageCacheException;
import com.typesafe.config.Config;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.Charset;

/**
 * Local cache for the messages
 * Created by hao on 10/6/16.
 */
public class MessageCache {

  private String id;
  private RocksDB db;
  private final Options options = new Options().setCreateIfMissing(true);

  private BloomF
  public MessageCache(String id) {
    this.id = id;
    try {
      db = RocksDB.open(options, id);
      db.keyMayExist()
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public void close() {
    if (null != db) {
      db.close();
    }
    options.close();
  }

  public void save(String key, String value) {
    try {
      db.put(key.getBytes(), value.getBytes());
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public void remove(String key) {
    try {
      db.remove(key.getBytes());
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public
}
