package com.orctom.pipeline.model;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQ;
import com.orctom.rmq.RMQConsumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Successors implements RMQConsumer {

  private ActorContext context;
  private ActorRef actor;
  private int size;
  private Map<String, GroupSuccessors> groups = new HashMap<>();

  public Successors(ActorContext context, ActorRef actor) {
    this.context = context;
    this.actor = actor;
  }

  public boolean isEmpty() {
    return 0 == size;
  }

  public int size() {
    return size;
  }

  public synchronized boolean addSuccessor(String role, ActorRef actorRef) {
    if (0 == size++) {
      RMQ.getInstance().subscribe("ready", this);
    }
    return addToGroup(role, actorRef);
  }

  private boolean addToGroup(String role, ActorRef actorRefs) {
    return getGroupSuccessors(role).addSuccessor(actorRefs);
  }

  private GroupSuccessors getGroupSuccessors(String role) {
    GroupSuccessors group = groups.get(role);
    if (null == group) {
      group = new GroupSuccessors(context);
      groups.put(role, group);
    }
    return group;
  }

  public synchronized void remove(ActorRef actorRef) {
    if (0 == --size) {
      RMQ.getInstance().unsubscribe("ready", this);
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
      return Ack.HALT;
    }
    for (GroupSuccessors groupSuccessors : groups.values()) {
      groupSuccessors.sendMessage(message, actor);
    }

    return Ack.DONE;
  }
}
