package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Pipe;

public class RoleB extends Pipe {

  @Override
  protected void onMessage(Message message) {
    System.out.println("[B] " + message);
    sendToSuccessor(message);
  }
}
