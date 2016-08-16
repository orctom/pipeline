package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * actors of created actor
 * Created by chenhao on 8/16/16.
 */
public class LocalActors implements Serializable {

  private Set<ActorRef> actors = new HashSet<>();

  public LocalActors(Set<ActorRef> actors) {
    this.actors = actors;
  }

  public Set<ActorRef> getActors() {
    return actors;
  }
}
