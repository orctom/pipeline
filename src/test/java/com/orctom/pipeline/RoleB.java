package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Pipe;
import com.orctom.pipeline.utils.SimpleMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RoleB extends Pipe {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoleB.class);

  private static final SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleB";

  @Override
  protected void onMessage(Message message) {
    LOGGER.trace("[B] " + message);
    sendToSuccessor(message);
    metrics.mark(KEY);
  }
}
