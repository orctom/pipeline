package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.RMQConsumer;

class MessageSender implements RMQConsumer {

  @Override
  public Ack onMessage(String message) {
    return null;
  }
}
