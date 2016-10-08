package com.orctom.pipeline.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.orctom.pipeline.exception.MessageCacheException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Local cache for the messages
 * Created by hao on 10/6/16.
 */
public class MessageCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageCache.class);

  private String id;

  private static final ColumnFamilyDescriptor COLUMN_FAMILY_DEFAULT = new ColumnFamilyDescriptor(
      RocksDB.DEFAULT_COLUMN_FAMILY,
      new ColumnFamilyOptions()
  );
  private static final ColumnFamilyDescriptor COLUMN_FAMILY_SENT = new ColumnFamilyDescriptor(
      "sent".getBytes(),
      new ColumnFamilyOptions()
  );
  private static final ColumnFamilyDescriptor COLUMN_FAMILY_ACKED = new ColumnFamilyDescriptor(
      "acknowledged".getBytes(),
      new ColumnFamilyOptions()
  );

  private static final List<ColumnFamilyDescriptor> COLUMN_FAMILY_DESCRIPTORS = Lists.newArrayList(
      COLUMN_FAMILY_DEFAULT, COLUMN_FAMILY_SENT, COLUMN_FAMILY_ACKED
  );

  private Map<ColumnFamilyDescriptor, WriteBatch> writeBatches = ImmutableMap.of(
      COLUMN_FAMILY_DEFAULT, new WriteBatch(),
      COLUMN_FAMILY_SENT, new WriteBatch(),
      COLUMN_FAMILY_ACKED, new WriteBatch()
  );

  private Map<ColumnFamilyDescriptor, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();

  private RocksDB db;
  private DBOptions options = new DBOptions().setCreateIfMissing(true);
  private WriteOptions writeOptions = new WriteOptions();

  public MessageCache(String id) {
    this.id = id;
    open();
    initBatchThread();
  }

  private void open() {
    try {
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      db = RocksDB.open(options, id, COLUMN_FAMILY_DESCRIPTORS, handles);
      initHandlers(handles);
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  private void initHandlers(List<ColumnFamilyHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
      columnFamilyHandles.put(COLUMN_FAMILY_DESCRIPTORS.get(i), handles.get(i));
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

  public void save(ColumnFamilyDescriptor family, String key, String value) {
    writeBatches.get(family).put(key.getBytes(), value.getBytes());
  }

  private void persist(WriteBatch batch) {
    LOGGER.debug("size: {}", batch.count());
    try {
      db.write(writeOptions, batch);
      writeBatches.clear();
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public void remove(ColumnFamilyDescriptor family, String key) {
    try {
      db.remove(columnFamilyHandles.get(family), key.getBytes());
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public void markAsSent(String key) {

  }
}
