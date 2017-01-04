package com.orctom.pipeline.model;

public class DataPipelineMessage extends PipelineMessage {

  private String data;

  public DataPipelineMessage(PipelineMessage msg, String data) {
    super(msg);
    this.data = data;
  }

  public String getData() {
    return data;
  }
}
