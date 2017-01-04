package com.orctom.pipeline.model;

/**
 * Message ack
 * Created by chenhao on 8/17/16.
 */
public class MessageAck {

  private String id;

  public MessageAck(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
