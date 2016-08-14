package com.orctom.pipeline.model;

import java.io.Serializable;

/**
 * Messages that ben transformed through the pipeline.
 * Created by chenhao on 8/3/16.
 */
public abstract class Message implements Serializable {

  private long timestamp;

  public Message(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
