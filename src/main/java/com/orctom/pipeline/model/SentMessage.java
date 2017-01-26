package com.orctom.pipeline.model;

import com.google.common.base.Strings;
import com.orctom.rmq.Message;

import java.util.Arrays;

import static com.orctom.pipeline.Constants.MAX_LEN_NAMES;

public class SentMessage extends Message {

  public SentMessage(String id, byte[] data) {
    super(id, data);
  }

  public SentMessage(String role, Message message) {
    super(message.getId(), null);
    byte[] roleBytes = Strings.padEnd(role, MAX_LEN_NAMES, ' ').getBytes();
    super.data = concatenateByteArrays(roleBytes, message.getData());
  }

  @Override
  public byte[] getData() {
    return super.getData();
  }

  public String getRole() {
    byte[] data = super.getData();
    return new String(Arrays.copyOfRange(data, 0, MAX_LEN_NAMES)).trim();
  }

  public byte[] getOriginalData() {
    byte[] data = super.getData();
    return Arrays.copyOfRange(data, MAX_LEN_NAMES, data.length);
  }

  private byte[] concatenateByteArrays(byte[] a, byte[] b) {
    byte[] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
}
