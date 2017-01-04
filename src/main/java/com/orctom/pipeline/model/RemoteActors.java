package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

/**
 * paths of created actor
 * Created by chenhao on 8/16/16.
 */
public class RemoteActors extends PipelineMessage {

  private String role;
  private List<ActorRef> actors = new ArrayList<>();

  public RemoteActors(String role, List<ActorRef> actors) {
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
