package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.utils.SimpleMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RoleC extends Outlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoleC.class);

  private static final SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleC";

  @Override
  protected void onMessage(Message message) {
    LOGGER.trace("[C] " + message);
    metrics.mark(KEY);
  }
}
