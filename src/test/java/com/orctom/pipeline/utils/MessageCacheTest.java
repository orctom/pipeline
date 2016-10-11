package com.orctom.pipeline.utils;

import com.orctom.pipeline.model.MessageEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
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
    return loop(cache.CF_DEFAULT);
  }

  private int loop(ColumnFamilyDescriptor descriptor) {
    RocksIterator iterator = cache.iterator(descriptor);
    int count = 0;
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      count++;
      LOGGER.trace(new String(iterator.key()) + " -> " + new String(iterator.value()));
    }
    LOGGER.info("count = {}, {}", count, new String(descriptor.columnFamilyName()));
    return count;
  }

  @Test
  public void testMessageProcessing() {
    int total = 100_000;
    CountDownLatch latch = new CountDownLatch(1);
    Thread addingThread = new Thread() {
      @Override
      public void run() {
        LOGGER.debug("adding thread started.");
        int count = 0;
        for (int i = 0; i < total; i++) {
          String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
          String value = RandomStringUtils.randomAlphanumeric(300);
          cache.add(key, value);
          sleepFor(RandomUtils.nextInt(0, 3));
          count++;
        }
        LOGGER.debug("adding thread stopped. {}", count);
        latch.countDown();
      }
    };
    Thread sendingThread = new Thread() {
      @Override
      public void run() {
        LOGGER.debug("sending thread started.");
        int loop = 5;
        int count = 0;
        while (loop > 0) {
          RocksIterator iterator = cache.iterator(cache.CF_DEFAULT);
          for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            String value = new String(iterator.value());
            process(key, value);
            cache.markAsSent(key, value);
            count++;
          }

          sleepFor(MessageCache.PERSIST_PERIOD);
          if (latch.getCount() == 0) {
            loop--;
          }
        }
        LOGGER.debug("sending thread stopped. {}", count);
      }
    };
    Thread ackThread = new Thread() {
      @Override
      public void run() {
        LOGGER.debug("ack thread started.");
        int loop = 10;
        int count = 0;
        while (loop > 0) {
          RocksIterator iterator = cache.iterator(cache.CF_SENT);
          for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            String value = new String(iterator.value());
            process(key, value);
            cache.markAsAcked(key, value);
            count++;
          }

          sleepFor(MessageCache.PERSIST_PERIOD);
          if (latch.getCount() == 0) {
            loop--;
          }
        }
        LOGGER.debug("ack thread stopped. {}", count);
      }
    };
    addingThread.start();
    sleepFor(MessageCache.PERSIST_PERIOD);
    sendingThread.start();
    sleepFor(MessageCache.PERSIST_PERIOD);
    ackThread.start();

    try {
      addingThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    loop(cache.CF_DEFAULT);
    loop(cache.CF_SENT);
    loop(cache.CF_ACKED);

    try {
      sendingThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    loop(cache.CF_DEFAULT);
    loop(cache.CF_SENT);
    loop(cache.CF_ACKED);

    try {
      ackThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    sleepFor(3 * 1000);
    loop(cache.CF_DEFAULT);
    loop(cache.CF_SENT);
    loop(cache.CF_ACKED);
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
    LOGGER.trace("processing: {} -> {}", key, value);
  }
}
