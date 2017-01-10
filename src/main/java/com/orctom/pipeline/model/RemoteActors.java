package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

/**
 * When <code>windtalker</code> receives this message from remote <code>windtalkers</code>,<br/>
 * it should notify local pipe actions, that the payload of the message (an actor list)<br/>
 * should be used as the successors of the date flow. <br/>
 * a.k.a. sending the processed messages to them.
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
