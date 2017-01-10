package com.orctom.pipeline.model;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

public class LocalMetricsCollectorActors extends PipelineMessage {

  private List<ActorRef> actors = new ArrayList<>();

  public LocalMetricsCollectorActors() {
    super();
  }

  public LocalMetricsCollectorActors(List<ActorRef> actors) {
    super();
    this.actors = actors;
  }

  public List<ActorRef> getActors() {
    return actors;
  }

  public void setActors(List<ActorRef> actors) {
    this.actors = actors;
  }

  public void add(ActorRef actor) {
    this.actors.add(actor);
  }
}
