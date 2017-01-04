package com.orctom.pipeline.sample;

import com.orctom.pipeline.model.PipelineMessage;

public class DummyPipelineMessage extends PipelineMessage {

  private String msg;

  public DummyPipelineMessage(String msg) {
    super();
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
