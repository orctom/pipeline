package com.orctom.pipeline.util;

import akka.actor.Actor;
import akka.actor.IndirectActorProducer;
import com.orctom.laputa.exception.IllegalArgException;
import org.springframework.context.ApplicationContext;

public class SpringActorProducer implements IndirectActorProducer {

  private final ApplicationContext applicationContext;
  private final Class<? extends Actor> actorBeanType;

  public SpringActorProducer(ApplicationContext applicationContext, Class<? extends Actor> actorBeanType) {
    verify(applicationContext, "applicationContext");
    verify(actorBeanType, "actorBeanType");

    this.applicationContext = applicationContext;
    this.actorBeanType = actorBeanType;
  }

  private void verify(Object obj, String name) {
    if (null == obj) {
      throw new IllegalArgException(name + " should not be null");
    }
  }

  @Override
  public Actor produce() {
    return applicationContext.getBean(actorBeanType);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Actor> actorClass() {
    return actorBeanType;
  }
}