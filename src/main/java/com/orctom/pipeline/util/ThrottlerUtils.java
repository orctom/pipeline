package com.orctom.pipeline.util;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.contrib.throttle.Throttler;
import akka.contrib.throttle.TimerBasedThrottler;
import com.orctom.pipeline.Pipeline;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static com.orctom.pipeline.Constants.PIPELINE_THROTTLE;

public abstract class ThrottlerUtils {

  private static int getThrottle() {
    return Pipeline.getInstance()
        .getConfig()
        .getInt(PIPELINE_THROTTLE);
  }

  public static ActorRef getThrottler(ActorRefFactory factory, ActorRef actorRef) {
    ActorRef throttler = factory.actorOf(
        Props.create(
            TimerBasedThrottler.class,
            new Throttler.Rate(getThrottle(), Duration.create(1, TimeUnit.SECONDS))
        )
    );
    throttler.tell(new Throttler.SetTarget(actorRef), null);

    return throttler;
  }
}
