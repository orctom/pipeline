package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.AbstractProcedure;
import com.orctom.pipeline.precedure.Hydrant;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RoleB extends AbstractProcedure {

  @Override
  protected String getPredecessorRoleName() {
    return "roleA";
  }

  @Override
  protected String getSuccessorRoleName() {
    return "roleC";
  }

  @Override
  protected void onMessage(Message message) {
    System.out.println("[B] " + message);
    sendToSuccessor(message);
  }
}
