package com.orctom.pipeline.model;

import com.orctom.rmq.Message;

public class SentMessage extends Message {

  private String role;
  private long originalId;

  public SentMessage(String role, long originalId, String id) {
    super(id, null);
    this.role = role;
    this.originalId = originalId;
  }

  public String getRole() {
    return role;
  }

  public long getOriginalId() {
    return originalId;
  }

  public void setData(byte[] data) {
    super.data = data;
  }
}
