package com.orctom.pipeline.util;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.orctom.laputa.model.Metric;
import com.orctom.laputa.model.MetricCallback;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.precedure.SimpleMetricsActor;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMetricCallback implements MetricCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMetricCallback.class);

  private static final SimpleMetricCallback INSTANCE = new SimpleMetricCallback();

  private ActorRef simpleMetricsActor;

  private SimpleMetricCallback() {
    ActorSystem system = Pipeline.getInstance().getSystem();
    Props props = Props.create(SimpleMetricsActor.class);
    simpleMetricsActor = system.actorOf(props, "simple-metrics");
  }

  public static SimpleMetricCallback getInstance() {
    return INSTANCE;
  }

  @Override
  public void onMetric(Metric metric) {
    byte[] data = SerilazationUtils.toBytes(metric);
    Message message = new Message(IdUtils.generate(), data);
    LOGGER.debug("onMetric: {}, {}", message.getId(), metric);
    simpleMetricsActor.tell(message, ActorRef.noSender());
  }
}
