package com.orctom.pipeline.util;

import akka.actor.ActorRef;
import com.orctom.laputa.model.Metric;
import com.orctom.laputa.model.MetricCallback;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.model.PipelineMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleMetricCallback implements MetricCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMetricCallback.class);

  private Set<ActorRef> metricsCollectorActors = new HashSet<>();

  private String roleName;

  public SimpleMetricCallback(String roleName) {
    this.roleName = roleName;
  }

  public void setMetricsCollectorActors(Set<ActorRef> metricsCollectorActors) {
    LOGGER.info("metrics collectors set.");
    this.metricsCollectorActors = metricsCollectorActors;
  }

  @Override
  public void onMetric(Metric metric) {
    if (null == metricsCollectorActors || metricsCollectorActors.isEmpty()) {
      LOGGER.trace("Skipped, no metrics collectors");
      return;
    }

    PipelineMetrics pm = new PipelineMetrics(Pipeline.getInstance().getApplicationName(), roleName, metric);
    LOGGER.debug("onMetric: {}", metric);
    for (ActorRef actor : metricsCollectorActors) {
      actor.tell(pm, ActorRef.noSender());
    }
  }

  public void addCollectors(List<ActorRef> actors) {
    LOGGER.info("metrics collectors added: {}.", actors);
    metricsCollectorActors.addAll(actors);
  }

  public void removeCollector(ActorRef actor) {
    LOGGER.info("metrics collector added: {}.", actor);
    metricsCollectorActors.remove(actor);
  }
}
