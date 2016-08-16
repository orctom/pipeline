package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.utils.SimpleMetrics;

import java.util.concurrent.TimeUnit;

public class RoleC extends Outlet {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleC";

  @Override
  protected void onMessage(Message message) {
    logger.trace("[C] " + message);
    metrics.mark(KEY);
  }
}
