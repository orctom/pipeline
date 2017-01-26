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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.orctom.pipeline.Constants.*;

public class Successors implements RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Successors.class);

  private ActorContext context;
  private ActorRef actor;
  private RMQ rmq;
  private SimpleMetrics metrics;
  private volatile int size;
  private Map<String, GroupSuccessors> groups = new ConcurrentHashMap<>();

  public Successors(ActorContext context, ActorRef actor, RMQ rmq, SimpleMetrics metrics) {
    this.context = context;
    this.actor = actor;
    this.rmq = rmq;
    this.metrics = metrics;
  }

  public synchronized boolean addSuccessor(String role, ActorRef actorRef) {
    LOGGER.debug("Added successor: {}, {}", role, actorRef);
    if (0 == size++) {
      LOGGER.info("Subscribed to 'ready'.");
      rmq.subscribe(Q_READY, this);
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
    LOGGER.debug("Removed successor: {}", actorRef);
    if (0 == --size) {
      rmq.unsubscribe(Q_READY, this);
      LOGGER.info("Un-subscribed from 'ready'.");
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
    try {
      if (groups.isEmpty()) {
        LOGGER.warn("No successors, halt.");
        return Ack.WAIT;
      }

      for (Map.Entry<String, GroupSuccessors> entry : groups.entrySet()) {
        String role = entry.getKey();
        GroupSuccessors groupSuccessors = entry.getValue();
        groupSuccessors.sendMessage(message, actor);
        markSentMessage(role, message);
      }

      metrics.mark(METER_SENT);

      return Ack.DONE;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Ack.LATER;
    }
  }

  private void markSentMessage(String role, Message message) {
      rmq.send(Q_SENT, new SentMessage(role, message));
  }

  public String getRoles() {
    return Arrays.toString(groups.keySet().toArray());
  }

  @Override
  public String toString() {
    return "Successors{" +
        "groups=" + groups +
        '}';
  }
}
