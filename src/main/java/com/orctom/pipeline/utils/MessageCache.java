package com.orctom.pipeline.utils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
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

  public static final int PERSIST_DELAY = 0;
  public static final int PERSIST_PERIOD = 1000;

  public final ColumnFamilyDescriptor CF_DEFAULT = new ColumnFamilyDescriptor(
      RocksDB.DEFAULT_COLUMN_FAMILY,
      createColumnFamilyOptions()
  );
  public final ColumnFamilyDescriptor CF_SENT = new ColumnFamilyDescriptor(
      "sent".getBytes(),
      createColumnFamilyOptions()
  );
  public final ColumnFamilyDescriptor CF_ACKED = new ColumnFamilyDescriptor(
      "acknowledged".getBytes(),
      createColumnFamilyOptions()
  );

  private final List<ColumnFamilyDescriptor> COLUMN_FAMILY_DESCRIPTORS = Lists.newArrayList(
      CF_DEFAULT, CF_SENT, CF_ACKED
  );

  private Map<ColumnFamilyDescriptor, WriteBatch> writeBatches = Maps.newConcurrentMap();

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
    initBatches();
    initBatchThread();
  }

  private ColumnFamilyOptions createColumnFamilyOptions() {
    return new ColumnFamilyOptions().setTableFormatConfig(
        new BlockBasedTableConfig().setFilter(new BloomFilter())
    );
  }

  private void initDB(String path) {
    ensureDataDirExist();
    try {
      initColumnFamilies(path);
    } catch (RocksDBException ignored) {
      // extra column families exist, continue open the db in column family approach.
    }
  }

  private void ensureDataDirExist() {
    File dataDir = new File(".", "data");
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    LOGGER.trace("ensuring data dir existence, created: {}", created);
  }

  private void initColumnFamilies(String path) throws RocksDBException {
    try (final Options opts = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opts, path)) {

      // creating column families
      db.createColumnFamily(CF_SENT);
      db.createColumnFamily(CF_ACKED);
    }
  }

  private void open(String path, int ttl) {
    try {
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      List<Integer> ttlList = Lists.newArrayList(ttl, ttl, ttl);
      db = TtlDB.open(options, path, COLUMN_FAMILY_DESCRIPTORS, handles, ttlList, false);
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

  private void initBatches() {
    for (ColumnFamilyDescriptor descriptor : COLUMN_FAMILY_DESCRIPTORS) {
      writeBatches.put(descriptor, new WriteBatch());
    }
  }

  private void initBatchThread() {
    Timer timer = new Timer(true);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Writing batch...");
        for (ColumnFamilyDescriptor descriptor : COLUMN_FAMILY_DESCRIPTORS) {
          WriteBatch writeBatch = writeBatches.put(descriptor, new WriteBatch());
          persist(writeBatch);
        }
      }
    }, PERSIST_DELAY, PERSIST_PERIOD);
  }

  private void persist(WriteBatch batch) {
    int size = batch.count();
    if (0 == size) {
      LOGGER.debug("batch size: 0, skipped.");
      return;
    }

    LOGGER.debug("batch size: {}", size);

    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      db.write(writeOptions, batch);
      stopwatch.stop();
      LOGGER.info(stopwatch.toString());
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public void close() {
    columnFamilyHandles.values().forEach(ColumnFamilyHandle::close);
    options.close();
    if (null != db) {
      db.close();
    }
  }

  public void dropColumnFamily(ColumnFamilyHandle handle) {
    try {
      db.dropColumnFamily(handle);
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public String get(ColumnFamilyDescriptor descriptor, String key) {
    try {
      return new String(db.get(getColumnFamilyHandle(descriptor), key.getBytes()));
    } catch (RocksDBException e) {
      throw new MessageCacheException(e.getMessage(), e);
    }
  }

  public MessageEntry get() {
    return get(CF_DEFAULT);
  }

  public MessageEntry get(ColumnFamilyDescriptor descriptor) {
    RocksIterator iterator = iterator(descriptor);
    iterator.seekToFirst();
    iterator.next();
    return iterator.isValid() ? new MessageEntry(iterator.key(), iterator.value()) : null;
  }

  public RocksIterator iterator() {
    return iterator(CF_DEFAULT);
  }

  public RocksIterator iterator(ColumnFamilyDescriptor descriptor) {
    return db.newIterator(getColumnFamilyHandle(descriptor));
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

  public void clearData(ColumnFamilyDescriptor descriptor) {
    RocksIterator iterator = iterator(descriptor);
    try {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        db.remove(getColumnFamilyHandle(descriptor), iterator.key());
      }
    } catch (RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private ColumnFamilyHandle getColumnFamilyHandle(ColumnFamilyDescriptor descriptor) {
    return columnFamilyHandles.get(descriptor);
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
