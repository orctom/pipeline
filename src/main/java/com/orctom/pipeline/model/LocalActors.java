package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;

/**
 * actors of created actor
 * Created by chenhao on 8/16/16.
 */
public class LocalActors extends PipelineMessage {

  private Map<ActorRef, Role> actors = new HashMap<>();

  public LocalActors() {
    super();
  }

  public LocalActors(Map<ActorRef, Role> actors) {
    super();
    this.actors = actors;
  }

  public Map<ActorRef, Role> getActors() {
    return actors;
  }

  public void addActor(ActorRef actor, Role role) {
    actors.put(actor, role);
  }
}
