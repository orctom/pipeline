package com.orctom.pipeline.sample;

import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.PipeActor;

import java.util.concurrent.TimeUnit;

class RoleB extends PipeActor {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  static final String ID = "roleB";

  @Override
  protected void onMessage(Message message) {
    logger.trace("[B] " + message);
    sendToSuccessors(message);
    metrics.mark(ID);
  }

  public static void main(String[] args) {
    String cluster = "dummy";
    final Pipeline pipeline = Pipeline.create(cluster, RoleB.ID, RoleA.ID);
    pipeline.createActor(RoleB.ID, RoleB.class);
    pipeline.start();
  }
}
