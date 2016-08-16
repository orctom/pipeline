package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Pipe;
import com.orctom.pipeline.utils.SimpleMetrics;

import java.util.concurrent.TimeUnit;

public class RoleB extends Pipe {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleB";

  @Override
  protected void onMessage(Message message) {
    logger.trace("[B] " + message);
    sendToSuccessor(message);
    metrics.mark(KEY);
  }
}
