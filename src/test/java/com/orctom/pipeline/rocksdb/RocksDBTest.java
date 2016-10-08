package com.orctom.pipeline.rocksdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.rocksdb.*;

public class RocksDBTest {
  private RocksDB db;
  private Statistics statistics;

  private void init() {
    final Options options = new Options();
    try {
      options.setCreateIfMissing(true)
          .createStatistics()
//          .setWriteBufferSize(8 * SizeUnit.KB)
//          .setMaxWriteBufferNumber(3)
//          .setMaxBackgroundCompactions(10)
//          .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
//          .setCompactionStyle(CompactionStyle.UNIVERSAL)
//          .setMemTableConfig(
//              new HashLinkedListMemTableConfig().setBucketCount(100000))
//          .setRateLimiterConfig(new GenericRateLimiterConfig(10000000))
      ;
      db = RocksDB.open(options, "rocks");
      statistics = options.statisticsPtr();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void close() {
    System.out.println("close");
    if (null != db) {
      db.close();
    }
    if (null != statistics) {
      System.out.println(statistics.getHistogramData(HistogramType.SOFT_RATE_LIMIT_DELAY_COUNT).getAverage());
    }
  }

  public void test() {
    init();
    long start = System.currentTimeMillis();
    try {
      for (int i = 0; i < 10; i++) {
        String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
//        String value = RandomStringUtils.randomAlphanumeric(300);
        String value = String.valueOf(i);
        db.put(key.getBytes(), value.getBytes());
      }
      long end = System.currentTimeMillis();
      System.out.println("===============" + (end - start));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close();
    }
  }

  public void loop() {
    init();
    RocksIterator iterator = null;
    try {
      iterator = db.newIterator();

      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        System.out.println(new String(iterator.key()) + " -> " + new String(iterator.value()));
        db.remove(iterator.key());
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (null != iterator) {
        iterator.close();
      }
      close();
    }
  }

  public static void main(String[] args) {
    RocksDBTest test = new RocksDBTest();
    test.test();
    test.loop();
  }
}
