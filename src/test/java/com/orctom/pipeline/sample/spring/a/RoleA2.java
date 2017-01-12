package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Pipe;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

@Actor("roleA2")
public class RoleA2 extends Pipe {

  @Override
  protected Ack onMessage(Message message) {
    sendToSuccessors(message);
    return Ack.DONE;
  }
}
