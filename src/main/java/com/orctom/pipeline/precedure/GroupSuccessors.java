package com.orctom.pipeline.precedure;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.orctom.pipeline.util.ThrottlerUtils;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class GroupSuccessors {

  private final Logger logger = LoggerFactory.getLogger(GroupSuccessors.class);

  private ActorContext context;
  private AtomicLong next = new AtomicLong();
  private List<ActorRef> successors = new CopyOnWriteArrayList<>();

  GroupSuccessors(ActorContext context) {
    this.context = context;
  }

  boolean addSuccessor(ActorRef actorRef) {
    ActorRef throttler = ThrottlerUtils.getThrottler(context, actorRef);
    return !successors.contains(throttler) && successors.add(throttler);
  }

  void remove(ActorRef actorRef) {
    successors.remove(actorRef);
  }

  void sendMessage(Message message, ActorRef sender) {
    if (successors.isEmpty()) {
      return;
    }
    int size = successors.size();
    int index = 1 == size ? 0 : (int) (next.getAndIncrement() % size);
    ActorRef successor = successors.get(index);
    successor.tell(message, sender);
  }

  @Override
  public String toString() {
    return successors.toString();
  }
}
