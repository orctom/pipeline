package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

/**
 * actors of created actor
 * Created by chenhao on 8/16/16.
 */
public class LocalActors extends Message {

  private String role;
  private List<ActorRef> actors = new ArrayList<>();

  public LocalActors(String role, List<ActorRef> actors) {
    super();
    this.role = role;
    this.actors = actors;
  }

  public String getRole() {
    return role;
  }

  public List<ActorRef> getActors() {
    return actors;
  }
}
