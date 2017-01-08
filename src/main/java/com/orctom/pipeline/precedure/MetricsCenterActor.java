package com.orctom.pipeline.precedure;

import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCenterActor extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCenterActor.class);

  static final String NAME = "metrics-center";

  @Override
  public void onReceive(Object message) throws Throwable {

  }
}
