package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQConsumer;

public class MessageConsumer implements RMQConsumer {

  private Pipe actor;

  public MessageConsumer(Pipe actor) {
    this.actor = actor;
  }

  @Override
  public Ack onMessage(Message message) {
    return actor.onMessage(message);
  }
}
