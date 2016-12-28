package com.orctom.pipeline.sample;

import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Outlet;

import java.util.concurrent.TimeUnit;

class RoleC extends Outlet {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  static final String ID = "roleC";

  @Override
  protected void onMessage(Message message) {
    logger.trace("[C] " + message);
    metrics.mark(ID);
  }

  public static void main(String[] args) {
    String cluster = "dummy";
    final Pipeline pipeline = Pipeline.create(cluster, RoleC.ID, RoleB.ID);
    pipeline.createActor(RoleC.ID, RoleC.class);
    pipeline.start();
  }
}
