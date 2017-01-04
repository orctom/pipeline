package com.orctom.pipeline.model;

import com.orctom.pipeline.util.IdUtils;

import java.io.Serializable;

/**
 * Messages that ben transformed through the pipeline.
 * Created by chenhao on 8/3/16.
 */
public abstract class PipelineMessage implements Serializable {

  private String id;
  private long timestamp;

  public PipelineMessage() {
    id = IdUtils.generate();
    timestamp = System.currentTimeMillis();
  }

  public PipelineMessage(PipelineMessage msg) {
    this.id = msg.getId();
    this.timestamp = msg.getTimestamp();
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
