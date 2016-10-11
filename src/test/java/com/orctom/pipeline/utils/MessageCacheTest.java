package com.orctom.pipeline.utils;

import com.orctom.pipeline.model.MessageEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.*;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class MessageCacheTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageCacheTest.class);

  private MessageCache cache;

  @Before
  public void before() {
    cache = new MessageCache("local");
  }

  @After
  public void after() {
    clearData();
    if (null != cache) {
      cache.close();
    }
  }

  private void clearData() {
    cache.clearData(cache.CF_DEFAULT);
    cache.clearData(cache.CF_SENT);
    cache.clearData(cache.CF_ACKED);
  }

  @Test
  public void testSave() {
    int total = 1_000_000;
    for (int i = 0; i < total; i++) {
      String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
      String value = RandomStringUtils.randomAlphanumeric(300);
      cache.add(key, value);
      LOGGER.trace("added {}: {} -> {}", i, key, value);
    }
    sleepFor(MessageCache.PERSIST_PERIOD);
    int count = loop();
    assertEquals(total, count);
  }

  private int loop() {
    LOGGER.info("looping...");
    RocksIterator iterator = cache.iterator(cache.CF_DEFAULT);
    int count = 0;
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      count++;
      LOGGER.trace(new String(iterator.key()) + " -> " + new String(iterator.value()));
    }
    LOGGER.info("count = {}", count);
    return count;
  }

  @Test
  public void testMessageCache() {
    Thread addThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 1_000; i++) {
          String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
          String value = RandomStringUtils.randomAlphanumeric(300);
          cache.add(key, value);
          sleepForAWhile();
        }
      }
    };
    Thread workerThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 1_000; i++) {
          MessageEntry msg = cache.get();
          if (null == msg) {
            LOGGER.info("emtpy message");
            continue;
          }
          process(msg.getKey(), msg.getValue());
          cache.markAsSent(msg.getKey(), msg.getValue());
        }
      }
    };
    addThread.start();
    workerThread.start();
    cache.debug();
    sleepForAWhile();
    cache.debug();
    sleepForAWhile();
    cache.debug();
    sleepForAWhile();
    cache.debug();
    sleepForAWhile();
    cache.debug();
    sleepForAWhile();
    cache.debug();
  }

  private void sleepForAWhile() {
    sleepFor(RandomUtils.nextInt(1, 500));
  }

  private void sleepFor(int milliseconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliseconds);
    } catch (InterruptedException ignored) {
    }
  }

  private void process(String key, String value) {
    LOGGER.info("processing: {} -> {}", key, value);
  }
}
