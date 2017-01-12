package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.HashMap;
import java.util.Map;

/**
 * When <code>windtalker</code> receives this message from remote <code>windtalkers</code>,<br/>
 * it should notify local pipe actions, that the payload of the message (an actor list)<br/>
 * should be used as the successors of the date flow. <br/>
 * a.k.a. sending the processed messages to them.
 */
public class RemoteActors extends PipelineMessage {

  private Map<ActorRef, Role> actors = new HashMap<>();

  public RemoteActors(Map<ActorRef, Role> actors) {
    super();
    this.actors = actors;
  }

  public Map<ActorRef, Role> getActors() {
    return actors;
  }
}
