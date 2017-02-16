package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.orctom.pipeline.util.RoleUtils;
import com.orctom.pipeline.util.ThrottlerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

public abstract class ActorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorFactory.class);

  private static ApplicationContext applicationContext;
  private static ActorSystem actorSystem;
  private static Map<String, ActorRef> cache = new HashMap<>();

  static void setApplicationContext(ApplicationContext applicationContext) {
    ActorFactory.applicationContext = applicationContext;
    ActorFactory.actorSystem = applicationContext.getBean(ActorSystem.class);
  }

  private static Props propsOf(String actorBeanName) {
    return Props.create(SpringActorProducer.class, ActorFactory.applicationContext, actorBeanName);
  }

  public static synchronized ActorRef actorOf(Class<? extends UntypedActor> actorBeanType) {
    ActorRef actor = create(actorBeanType);
    return ThrottlerUtils.getThrottler(actorSystem, actor);
  }

  static ActorRef create(Class<? extends UntypedActor> actorBeanType) {
    final String actorBeanName = RoleUtils.getRole(actorBeanType).getRole();
    ActorRef actor = cache.computeIfAbsent(actorBeanName, ActorFactory::create);
    if (actor.isTerminated()) {
      LOGGER.warn("Actor: {} terminated, recreating.", actorBeanType);
      actor = create(actorBeanName);
      cache.put(actorBeanName, actor);
    }
    return actor;
  }

  private static ActorRef create(String actorBeanName) {
    LOGGER.info("Created actor: {}", actorBeanName);
    return actorSystem.actorOf(propsOf(actorBeanName), actorBeanName);
  }
}
