package com.orctom.pipeline.model;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQ;
import com.orctom.rmq.RMQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.orctom.pipeline.Constants.*;

public class Successors implements RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Successors.class);

  private ActorContext context;
  private ActorRef actor;
  private SimpleMetrics metrics;
  private int size;
  private Map<String, GroupSuccessors> groups = new HashMap<>();

  public Successors(ActorContext context, ActorRef actor, SimpleMetrics metrics) {
    this.context = context;
    this.actor = actor;
    this.metrics = metrics;
  }

  public boolean isEmpty() {
    return 0 == size;
  }

  public int size() {
    return size;
  }

  public synchronized boolean addSuccessor(String role, ActorRef actorRef) {
    if (0 == size++) {
      RMQ.getInstance().subscribe(Q_READY, this);
    }
    return addToGroup(role, actorRef);
  }

  private boolean addToGroup(String role, ActorRef actorRefs) {
    return getGroupSuccessors(role).addSuccessor(actorRefs);
  }

  private GroupSuccessors getGroupSuccessors(String role) {
    return groups.computeIfAbsent(role, k -> new GroupSuccessors(context));
  }

  public synchronized void remove(ActorRef actorRef) {
    if (0 == --size) {
      RMQ.getInstance().unsubscribe(Q_READY, this);
    }
    for (GroupSuccessors groupSuccessors : groups.values()) {
      groupSuccessors.remove(actorRef);
    }
  }

  public Collection<GroupSuccessors> getGroups() {
    return groups.values();
  }

  @Override
  public Ack onMessage(Message message) {
    if (isEmpty()) {
      LOGGER.warn("No successors, halt.");
      return Ack.HALT;
    }
    metrics.mark(METER_SENT);
    for (GroupSuccessors groupSuccessors : groups.values()) {
      groupSuccessors.sendMessage(message, actor);
    }

    moveToSentQueue(message);
    return Ack.DONE;
  }

  private void moveToSentQueue(Message message) {
    RMQ.getInstance().send(Q_SENT, message);
  }

  @Override
  public String toString() {
    return "Successors{" +
        "groups=" + groups +
        '}';
  }
}
