package com.orctom.pipeline.rocksdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class RocksDBTest {
  private RocksDB db;
  private Statistics statistics;

  private List<ImmutablePair<String, String>> data = new ArrayList<>(1_000_000);

  private void init() {
    try {
      Options options = new Options().setCreateIfMissing(true).createStatistics();
      db = RocksDB.open(options, "rocks");
      statistics = options.statisticsPtr();
    } catch (Exception e) {
      e.printStackTrace();
    }

    initData();
  }

  private void initData() {
    for (int i = 0; i < 1_000_000; i++) {
      String key = String.valueOf(i) + "_" + RandomStringUtils.randomAlphanumeric(8);
      String value = RandomStringUtils.randomAlphanumeric(300);
      data.add(new ImmutablePair<>(key, value));
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
      for (ImmutablePair<String, String> entry : data) {
        db.put(entry.getLeft().getBytes(), entry.getRight().getBytes());
      }
      long end = System.currentTimeMillis();
      System.out.println("===============" + (end - start));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close();
    }
  }

  public void testBatch() {
    init();
    long start = System.currentTimeMillis();
    WriteOptions writeOpt = new WriteOptions();
    try {
      WriteBatch batch = new WriteBatch();
      for (ImmutablePair<String, String> entry : data) {
        batch.put(entry.getLeft().getBytes(), entry.getRight().getBytes());
      }
      db.write(writeOpt, batch);
      long end = System.currentTimeMillis();
      System.out.println("===============" + (end - start));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close();
    }
  }

  public void testBatch2() {
    init();
    long start = System.currentTimeMillis();
    WriteOptions writeOpt = new WriteOptions();
    try {
      WriteBatch batch = new WriteBatch();
      int i = 0;
      for (ImmutablePair<String, String> entry : data) {
        i++;
        batch.put(entry.getLeft().getBytes(), entry.getRight().getBytes());
        if (i % 100 == 0) {
          db.write(writeOpt, batch);
          batch = new WriteBatch();
        }
      }
      db.write(writeOpt, batch);
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
//    test.test();
//    test.testBatch();
    test.testBatch2();
//    test.loop();
  }
}
