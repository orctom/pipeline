package com.orctom.pipeline.utils;

import com.orctom.pipeline.model.MessageEntry;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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
    cache.close();
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
          process(msg.getKey(), msg.getValue());
          cache.markAsSent(msg.getKey(), msg.getValue());
        }
      }
    };
  }

  private void sleepForAWhile() {
    try {
      TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(1, 500));
    } catch (InterruptedException ignored) {
    }
  }

  private void process(String key, String value) {
    LOGGER.info("processing: {} -> {}", key, value);
  }
}
