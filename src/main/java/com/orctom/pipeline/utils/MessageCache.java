package com.orctom.pipeline.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.orctom.pipeline.exception.MessageCacheException;
import com.orctom.pipeline.model.MessageEntry;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Local cache for the messages
 * Created by hao on 10/6/16.
 */
public class MessageCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageCache.class);

  public static final ColumnFamilyDescriptor CF_DEFAULT = new ColumnFamilyDescriptor(
      RocksDB.DEFAULT_COLUMN_FAMILY,
      createColumnFamilyOptions()
  );
  public static final ColumnFamilyDescriptor CF_SENT = new ColumnFamilyDescriptor(
      "sent".getBytes(),
      createColumnFamilyOptions()
  );
  public static final ColumnFamilyDescriptor CF_ACKED = new ColumnFamilyDescriptor(
      "acknowledged".getBytes(),
      createColumnFamilyOptions()
  );

  private static final List<ColumnFamilyDescriptor> COLUMN_FAMILY_DESCRIPTORS = Lists.newArrayList(
      CF_DEFAULT, CF_SENT, CF_ACKED
  );

  private Map<ColumnFamilyDescriptor, WriteBatch> writeBatches = ImmutableMap.of(
      CF_DEFAULT, new WriteBatch(),
      CF_SENT, new WriteBatch(),
      CF_ACKED, new WriteBatch()
  );

  private Map<ColumnFamilyDescriptor, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();

  private TtlDB db;
  private DBOptions options = new DBOptions();
  private WriteOptions writeOptions = new WriteOptions();

  public MessageCache(String id) {
    this(id, 1800);
  }

  public MessageCache(String id, int ttl) {
    String path = "data/" + id;
    initDB(path);
    open(path, ttl);
    initBatchThread();
  }

  private static ColumnFamilyOptions createColumnFamilyOptions() {
    return new ColumnFamilyOptions().setTableFormatConfig(
        new BlockBasedTableConfig().setFilter(new BloomFilter())
    );
  }

  private void initDB(String path) {
    ensureDataDirExist();
    File dbDir = new File(".", path);
    if (dbDir.exists()) {
      return;
    }
    initColumnFamilies(path);
  }

  private void ensureDataDirExist() {
    File dataDir = new File(".", "data");
    if (dataDir.exists()) {
      return;
    }
    dataDir.mkdirs();
  }

  private void initColumnFamilies(String path) {
    try (final Options opts = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opts, path)) {

      // create column family
      db.createColumnFamily(CF_SENT);
      db.createColumnFamily(CF_ACKED);
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  private void open(String path, int ttl) {
    try {
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      List<Integer> ttls = Lists.newArrayList(ttl, ttl, ttl);
      db = TtlDB.open(options, path, COLUMN_FAMILY_DESCRIPTORS, handles, ttls, false);
      initHandlerMap(handles);
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  private void initHandlerMap(List<ColumnFamilyHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
      ColumnFamilyDescriptor descriptor = COLUMN_FAMILY_DESCRIPTORS.get(i);
      columnFamilyHandles.put(descriptor, handles.get(i));
    }
  }

  private void initBatchThread() {
    Timer timer = new Timer(true);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Writing batch...");
        writeBatches.values().forEach(MessageCache.this::persist);
      }
    }, 0, 1000);
  }

  public void close() {
    columnFamilyHandles.values().forEach(ColumnFamilyHandle::close);
    options.close();
    if (null != db) {
      db.close();
    }
  }

  private void persist(WriteBatch batch) {
    LOGGER.debug("size: {}", batch.count());
    try {
      db.write(writeOptions, batch);
      writeBatches.values().forEach(WriteBatch::clear);
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public String get(ColumnFamilyDescriptor family, String key) {
    try {
      return new String(db.get(columnFamilyHandles.get(family), key.getBytes()));
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public MessageEntry get() {
    RocksIterator iterator = iterator();
    iterator.seekToFirst();
    iterator.next();
    return iterator.isValid() ? new MessageEntry(iterator.key(), iterator.value()) : null;
  }

  public RocksIterator iterator() {
    return db.newIterator();
  }

  public void add(String key, String value) {
    add(CF_DEFAULT, key, value);
  }

  public void add(ColumnFamilyDescriptor family, String key, String value) {
    writeBatches.get(family).put(key.getBytes(), value.getBytes());
  }

  public boolean isDuplicated(String key) {
    byte[] keyBytes = key.getBytes();
    StringBuffer buffer = new StringBuffer();
    for (ColumnFamilyHandle handle : columnFamilyHandles.values()) {
      boolean exist = db.keyMayExist(handle, keyBytes, buffer);
      if (exist) {
        return true;
      }
    }

    return false;
  }

  public void remove(ColumnFamilyDescriptor family, String key) {
    writeBatches.get(family).remove(key.getBytes());
  }

  public void debug() {
    LOGGER.debug("debug start...");
    try {
      RocksIterator iterator = db.newIterator();
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        LOGGER.debug(new String(iterator.key()) + " -> " + new String(iterator.value()));
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
    LOGGER.debug("debug stop.");
  }

  public void markAsSent(String key, String value) {
    LOGGER.debug("mark as sent: {}", key);
    remove(CF_DEFAULT, key);
    add(CF_SENT, key, value);
  }

  public void markAsAcked(String key, String value) {
    LOGGER.debug("mark as acked: {}", key);
    remove(CF_SENT, key);
    add(CF_ACKED, key, value);
  }
}
