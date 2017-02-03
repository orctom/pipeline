package com.orctom.pipeline.model;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.persist.MessageQueue;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.orctom.pipeline.Constants.*;

public class Successors implements RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(Successors.class);

  private ActorContext context;
  private ActorRef actor;
  private MessageQueue messageQueue;
  private SimpleMetrics metrics;
  private volatile int size;
  private Map<String, GroupSuccessors> groups = new ConcurrentHashMap<>();

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("pipeline-sent@" + hashCode()).build()
  );

  public Successors(ActorContext context, ActorRef actor, MessageQueue messageQueue, SimpleMetrics metrics) {
    this.context = context;
    this.actor = actor;
    this.messageQueue = messageQueue;
    this.metrics = metrics;

    scheduleResendUnAckedMessages();
  }

  public synchronized boolean addSuccessor(String role, ActorRef actorRef) {
    LOGGER.debug("Added successor: {}, {}", role, actorRef);
    if (0 == size++) {
      LOGGER.info("Subscribed to 'ready'.");
      messageQueue.subscribe(Q_PROCESSED, this);
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
      messageQueue.unsubscribe(Q_PROCESSED, this);
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

      groups.entrySet().forEach(entry -> {
        String role = entry.getKey();
        String id = message.getId() + AT_SIGN + role;
        Message msg = new Message(id, message.getData());
        GroupSuccessors groupSuccessors = entry.getValue();
        groupSuccessors.sendMessage(msg, actor);

        recordSentMessage(id);
      });

      messageQueue.push(Q_SENT, message);

      metrics.mark(METER_SENT);

      return Ack.DONE;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Ack.LATER;
    }
  }

  private void recordSentMessage(String id) {
    messageQueue.push(Q_SENT_RECORDS, id, EMPTY_STRING);
  }

  private void scheduleResendUnAckedMessages() {
    scheduler.scheduleWithFixedDelay(() -> messageQueue.iterateSentMessages(message -> {
      String role = message.getRole();
      GroupSuccessors successors = groups.get(role);
      if (null == successors) {
        return;
      }
      successors.sendMessage(message, actor);
    }), 0, 30, TimeUnit.SECONDS);
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
