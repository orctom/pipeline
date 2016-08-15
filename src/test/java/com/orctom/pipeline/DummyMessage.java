package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;

public class DummyMessage extends Message {

  private String msg;

  public DummyMessage(String msg) {
    super(System.currentTimeMillis());
    this.msg = msg;
  }

  public String getMsg() {
    return msg;
  }

  @Override
  public String toString() {
    return "DummyMessage{" +
        "msg='" + msg + '\'' +
        '}';
  }
}
