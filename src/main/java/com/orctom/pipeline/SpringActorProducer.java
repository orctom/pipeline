package com.orctom.pipeline;

import akka.actor.Actor;
import akka.actor.IndirectActorProducer;
import com.orctom.laputa.exception.IllegalArgException;
import org.springframework.context.ApplicationContext;

public class SpringActorProducer implements IndirectActorProducer {

  private final ApplicationContext applicationContext;
  private final String actorBeanName;

  public SpringActorProducer(ApplicationContext applicationContext, String actorBeanName) {
    verify(applicationContext, "applicationContext");
    verify(actorBeanName, "actorBeanName");

    this.applicationContext = applicationContext;
    this.actorBeanName = actorBeanName;
  }

  private void verify(Object obj, String name) {
    if (null == obj) {
      throw new IllegalArgException(name + " should not be null");
    }
  }

  @Override
  public Actor produce() {
    return (Actor) applicationContext.getBean(actorBeanName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Actor> actorClass() {
    return (Class<? extends Actor>) applicationContext.getType(actorBeanName);
  }
}
