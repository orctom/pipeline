package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.HashSet;
import java.util.Set;

/**
 * paths of created actor
 * Created by chenhao on 8/16/16.
 */
public class RemoteActors extends Message {

  private Set<ActorRef> actors = new HashSet<>();

  public RemoteActors(Set<ActorRef> actors) {
    super();
    this.actors = actors;
  }

  public Set<ActorRef> getActors() {
    return actors;
  }
}
