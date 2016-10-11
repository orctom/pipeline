package com.orctom.pipeline.utils;

import com.orctom.pipeline.model.MessageEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MessageCacheTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageCacheTest.class);

  private static MessageCache cache;

  @BeforeClass
  public static void beforeClass() {
    cache = new MessageCache("local");
  }

  @AfterClass
  public static void afterClass() {
    if (null != cache) {
      cache.close();
    }
  }

  @Test
  public void testSave() {
    for (int i = 0; i < 1_000; i++) {
      String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
      String value = RandomStringUtils.randomAlphanumeric(300);
      cache.add(key, value);
      LOGGER.info("added {}: {} -> {}", i, key, value);
    }
    sleepFor(1000*5);
    loop();
    sleepFor(1000*10);
  }

  private void loop() {
    LOGGER.info("looping...");
    RocksIterator iterator = cache.iterator();
    int count = 0;
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      count++;
      LOGGER.info(new String(iterator.key()) + " -> " + new String(iterator.value()));
    }
    LOGGER.info("count = {}", count);
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
