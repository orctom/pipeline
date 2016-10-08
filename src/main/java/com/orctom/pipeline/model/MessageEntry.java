package com.orctom.pipeline.model;

public class MessageEntry {

  private String key;
  private String value;

  public MessageEntry(byte[] key, byte[] value) {
    this.key = new String(key);
    this.value = new String(value);
  }

  public MessageEntry(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
