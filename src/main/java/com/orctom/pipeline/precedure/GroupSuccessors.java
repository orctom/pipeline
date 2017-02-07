package com.orctom.pipeline.precedure;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.contrib.throttle.Throttler;
import akka.contrib.throttle.TimerBasedThrottler;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GroupSuccessors {

  private final Logger logger = LoggerFactory.getLogger(GroupSuccessors.class);

  private static final int THROTTLE_RATE = 10_000;

  private AtomicLong next = new AtomicLong();
  private List<ActorRef> successors = new CopyOnWriteArrayList<>();

  private LoadingCache<ActorRef, ActorRef> throttlers;

  GroupSuccessors(ActorContext context) {
    iniThrottlers(context);
  }

  boolean addSuccessor(ActorRef actorRef) {
    return !successors.contains(actorRef) && successors.add(actorRef);
  }

  public void addSuccessors(List<ActorRef> actorRefs) {
    successors.addAll(actorRefs);
  }

  void remove(ActorRef actorRef) {
    successors.remove(actorRef);
  }

  private void iniThrottlers(final ActorContext context) {
    throttlers = CacheBuilder
        .newBuilder()
        .weakValues()
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

  void sendMessage(Message message, ActorRef sender) {
    if (successors.isEmpty()) {
      return;
    }
    int size = successors.size();
    int index = (int) (next.getAndIncrement() % size);
    ActorRef successor = successors.get(index);
    try {
      throttlers.get(successor).tell(message, sender);
    } catch (ExecutionException e) {
      logger.error(e.getMessage(), e);
    }
  }

  @Override
  public String toString() {
    return successors.toString();
  }
}
