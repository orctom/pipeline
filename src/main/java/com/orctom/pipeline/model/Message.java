package com.orctom.pipeline.model;

import com.orctom.pipeline.util.IdUtils;

import java.io.Serializable;

/**
 * Messages that ben transformed through the pipeline.
 * Created by chenhao on 8/3/16.
 */
public abstract class Message implements Serializable {

  private long id;
  private long timestamp;

  public Message() {
    id = IdUtils.generate();
    timestamp = System.currentTimeMillis();
  }

  public Message(Message msg) {
    id = msg.getId();
    timestamp = msg.getTimestamp();
  }

  public long getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
