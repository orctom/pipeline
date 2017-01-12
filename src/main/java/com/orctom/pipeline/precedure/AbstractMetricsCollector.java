package com.orctom.pipeline.precedure;

import akka.actor.UntypedActor;
import com.orctom.pipeline.model.PipelineMetrics;

public abstract class AbstractMetricsCollector extends UntypedActor {

  public static final String ROLE = "metrics-collector";

  @Override
  public final void onReceive(Object message) throws Throwable {
    if (message instanceof PipelineMetrics) {
      onMessage((PipelineMetrics) message);
    } else {
      unhandled(message);
    }
  }

  public abstract void onMessage(PipelineMetrics metric);
}
