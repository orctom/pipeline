package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQConsumer;

public class InboxConsumer implements RMQConsumer {

  private PipeActor actor;

  public InboxConsumer(PipeActor actor) {
    this.actor = actor;
  }

  @Override
  public Ack onMessage(Message message) {
    return actor.onMsg(message);
  }

  @Override
  public String toString() {
    return "MessageConsumer{" + actor.toString() + '}';
  }
}
