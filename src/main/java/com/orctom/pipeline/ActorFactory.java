package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.orctom.pipeline.util.SpringActorProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

public abstract class ActorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActorFactory.class);

  private static ApplicationContext applicationContext;
  private static Map<Class<? extends UntypedActor>, ActorRef> cache = new HashMap<>();

  static void setApplicationContext(ApplicationContext applicationContext) {
    ActorFactory.applicationContext = applicationContext;
  }

  private static Props propsOf(Class<? extends UntypedActor> actorBeanType) {
    return Props.create(SpringActorProducer.class, ActorFactory.applicationContext, actorBeanType);
  }

  public static synchronized ActorRef actorOf(Class<? extends UntypedActor> actorBeanType) {
    ActorRef actor = cache.computeIfAbsent(actorBeanType, ActorFactory::create);
    if (actor.isTerminated()) {
      actor = create(actorBeanType);
      cache.put(actorBeanType, actor);
    }
    return actor;
  }

  private static ActorRef create(Class<? extends UntypedActor> actorBeanType) {
    ActorSystem actorSystem = applicationContext.getBean(ActorSystem.class);
    String name = firstCharLowerCased(actorBeanType.getSimpleName());
    LOGGER.info("Created actor: {} of type: {}", name, actorBeanType);
    return actorSystem.actorOf(propsOf(actorBeanType), name);
  }

  private static String firstCharLowerCased(String name) {
    char c[] = name.toCharArray();
    c[0] += 32;
    return new String(c);
  }
}
