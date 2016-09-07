package com.orctom.pipeline.model;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.contrib.throttle.Throttler;
import akka.contrib.throttle.TimerBasedThrottler;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GroupSuccessors {

  private static final Logger LOGGER = LoggerFactory.getLogger(GroupSuccessors.class);

  private static final int THROTTLE_RATE = 5000;

  private AtomicLong next = new AtomicLong();
  private List<ActorRef> successors = new ArrayList<>();

  private LoadingCache<ActorRef, ActorRef> throttlers;

  public GroupSuccessors(ActorContext context) {
    iniThrottlers(context);
  }

  public boolean addSuccessor(ActorRef actorRef) {
    return !successors.contains(actorRef) && successors.add(actorRef);
  }

  public void addSuccessors(List<ActorRef> actorRefs) {
    successors.addAll(actorRefs);
  }

  public void remove(ActorRef actorRef) {
    successors.remove(actorRef);
  }

  private void iniThrottlers(final ActorContext context) {
    throttlers = CacheBuilder
        .newBuilder()
        .softValues()
        .build(new CacheLoader<ActorRef, ActorRef>() {
          @Override
          public ActorRef load(ActorRef successor) throws Exception {
            ActorRef throttler = context.actorOf(
                Props.create(
                    TimerBasedThrottler.class,
                    new Throttler.Rate(THROTTLE_RATE, Duration.create(1, TimeUnit.SECONDS)))
            );
            throttler.tell(new Throttler.SetTarget(successor), null);
            return throttler;
          }
        });
  }

  public void sendMessage(Message message, ActorRef sender) {
    if (successors.isEmpty()) {
      return;
    }
    int size = successors.size();
    int index = (int) (next.getAndIncrement() % size);
    ActorRef successor = successors.get(index);
    try {
      throttlers.get(successor).tell(message, sender);
    } catch (ExecutionException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  @Override
  public String toString() {
    return successors.toString();
  }
}
