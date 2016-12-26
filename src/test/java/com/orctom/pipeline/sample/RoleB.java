package com.orctom.pipeline.sample;

import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Pipe;

import java.util.concurrent.TimeUnit;

public class RoleB extends Pipe {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleB";

  @Override
  protected void onMessage(Message message) {
    logger.trace("[B] " + message);
    sendToSuccessors(message);
    metrics.mark(KEY);
  }
}
