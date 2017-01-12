package com.orctom.pipeline.model;

import akka.actor.ActorRef;

public class SuccessorActor extends PipelineMessage {

  private String role;
  private ActorRef actor;

  public SuccessorActor(String role, ActorRef actor) {
    super();
    this.role = role;
    this.actor = actor;
  }

  public String getRole() {
    return role;
  }

  public ActorRef getActor() {
    return actor;
  }
}
