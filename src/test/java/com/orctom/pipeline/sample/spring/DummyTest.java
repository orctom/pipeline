package com.orctom.pipeline.sample.spring;

public class DummyTest {

  public static void main(String[] args) {
    for(byte b : "inbox".getBytes()) {
      System.out.println((char)b);
    }
  }
}
