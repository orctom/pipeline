package com.orctom.pipeline.util;

import akka.actor.*;
import org.springframework.context.ApplicationContext;

public class ActorFactory implements Extension {

  private ApplicationContext applicationContext;

  public ActorFactory(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  private Props propsOf(Class<? extends UntypedActor> actorBeanType) {
    return Props.create(SpringActorProducer.class, this.applicationContext, actorBeanType);
  }

  public ActorRef create(Class<? extends UntypedActor> actorBeanType) {
    ActorSystem actorSystem = applicationContext.getBean(ActorSystem.class);
    return actorSystem.actorOf(propsOf(actorBeanType));
  }
}
