package com.orctom.pipeline.util;

import akka.actor.Actor;
import akka.actor.IndirectActorProducer;
import org.springframework.context.ApplicationContext;

public class DependencyInjector implements IndirectActorProducer {

  private final ApplicationContext context;

  public DependencyInjector(ApplicationContext context) {
    this.context = context;
  }

  @Override
  public Actor produce() {
    return null;
  }

  @Override
  public Class<? extends Actor> actorClass() {
    return null;
  }
}
