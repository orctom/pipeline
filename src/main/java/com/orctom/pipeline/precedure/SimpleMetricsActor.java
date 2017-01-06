package com.orctom.pipeline.precedure;

import com.orctom.laputa.model.Metric;
import com.orctom.pipeline.util.SerilazationUtils;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMetricsActor extends Pipe {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMetricsActor.class);

  @Override
  protected Ack onMessage(Message message) {
    String id = message.getId();
    byte[] data = message.getData();
    Metric metric = SerilazationUtils.toObject(data, Metric.class);
    LOGGER.debug("id: {}, metric: {}", id, metric);
    return Ack.DONE;
  }
}
