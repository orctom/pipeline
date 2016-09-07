package com.orctom.pipeline.model;

import akka.actor.ActorContext;
import akka.actor.ActorRef;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Successors {

  private ActorContext context;
  private int size;
  private Map<String, GroupSuccessors> groups = new HashMap<>();

  public Successors(ActorContext context) {
    this.context = context;
  }

  public boolean isEmpty() {
    return 0 == size;
  }

  public int size() {
    return size;
  }

  public synchronized boolean addSuccessor(String role, ActorRef actorRef) {
    size++;
    return addToGroup(role, actorRef);
  }

  public synchronized void addSuccessors(String role, List<ActorRef> actorRefs) {
    size += actorRefs.size();
    addToGroup(role, actorRefs);
  }

  private boolean addToGroup(String role, ActorRef actorRefs) {
    return getGroupSuccessors(role).addSuccessor(actorRefs);
  }

  private void addToGroup(String role, List<ActorRef> actorRefs) {
    getGroupSuccessors(role).addSuccessors(actorRefs);
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
    size--;
    for (GroupSuccessors groupSuccessors : groups.values()) {
      groupSuccessors.remove(actorRef);
    }
  }

  public Collection<GroupSuccessors> getGroups() {
    return groups.values();
  }
}
