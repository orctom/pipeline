package com.orctom.pipeline.leveldb;

import org.apache.commons.lang3.RandomStringUtils;
import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;

import java.io.File;
import java.io.IOException;

public class LevelDBTest {

  private DB db;

  private void init() {
    Options options = new Options();
    options.createIfMissing(true);
    try {
      db = factory.open(new File("example"), options);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void close() {
    System.out.println("close");
    try {
      if (null != db) {
        db.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void test() {
    init();
    long start = System.currentTimeMillis();
    try {
      for (int i = 0; i < 1_000_000; i++) {
        String key = RandomStringUtils.randomAlphanumeric(8);
        String value = RandomStringUtils.randomAlphanumeric(300);
        db.put(bytes(key), bytes(value));
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
    DBIterator iterator = db.iterator();
    try {
      for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
        String key = asString(iterator.peekNext().getKey());
        String value = asString(iterator.peekNext().getValue());
        System.out.println(key + " = " + value);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        iterator.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      close();
    }
  }

  public static void main(String[] args) {
    new LevelDBTest().test();
//    new LevelDBTest().loop();
  }
}
