package com.orctom.pipeline.model;

public class DataMessage extends Message {

  private String data;

  public DataMessage(Message msg, String data) {
    super(msg);
    this.data = data;
  }

  public String getData() {
    return data;
  }
}
