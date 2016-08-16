package com.orctom.pipeline.model;

import akka.actor.ActorPath;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * paths of created actor
 * Created by chenhao on 8/16/16.
 */
public class RemoteActors implements Serializable {

  private Set<ActorRef> actors = new HashSet<>();

  public RemoteActors(Set<ActorRef> actors) {
    this.actors = actors;
  }

  public Set<ActorRef> getActors() {
    return actors;
  }
}
