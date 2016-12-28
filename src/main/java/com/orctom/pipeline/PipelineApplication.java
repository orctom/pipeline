package com.orctom.pipeline;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.orctom.laputa.exception.ClassLoadingException;
import com.orctom.laputa.exception.IllegalArgException;
import com.orctom.laputa.utils.ClassUtils;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.precedure.PipeActor;
import com.orctom.pipeline.util.ActorFactory;
import com.orctom.pipeline.util.IdUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PipelineApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineApplication.class);

  private static final PipelineApplication INSTANCE = new PipelineApplication();

  private String cluster;
  private String role;
  private Set<String> predecessors;
  private String[] basePackages;
  private AnnotationConfigApplicationContext applicationContext;
  private ActorSystem system;

  private List<ActorRef> actors = new ArrayList<>();

  private PipelineApplication() {
  }

  public static PipelineApplication getInstance() {
    return INSTANCE;
  }

  public PipelineApplication withCluster(String cluster) {
    this.cluster = cluster;
    return this;
  }

  public PipelineApplication withRole(String role) {
    this.role = role;
    return this;
  }

  public PipelineApplication withPredecessors(String... predecessors) {
    this.predecessors = Sets.newHashSet(predecessors);
    return this;
  }

  public void run(Class<?> configurationClass) {
    IdUtils.generate();
    validate();
    validate(configurationClass);
    createApplicationContext(configurationClass);
    createActorSystem();
    registerActors();
    start();
  }

  private void validate() {
    if (Strings.isNullOrEmpty(cluster)) {
      throw new IllegalArgException("`cluster` is required.");
    }
    if (Strings.isNullOrEmpty(role)) {
      throw new IllegalArgException("`role` is required.");
    }
  }

  private void validate(Class<?> configurationClass) {
    if (null == configurationClass) {
      throw new IllegalArgException("Null class to 'run()'!");
    }
    if (!configurationClass.isAnnotationPresent(Configuration.class)) {
      throw new IllegalArgException("@Configuration is expected on class: " + configurationClass);
    }

    ComponentScan componentScan = configurationClass.getAnnotation(ComponentScan.class);
    if (null == componentScan) {
      throw new IllegalArgException("@ComponentScan is expected on class: " + configurationClass);
    } else {
      basePackages = componentScan.basePackages();
    }

  }

  private void createApplicationContext(Class<?> configurationClass) {
    applicationContext = new AnnotationConfigApplicationContext(configurationClass);
  }

  private void createActorSystem() {
    Config config = Configurator.getInstance(role).getConfig();
    system = ActorSystem.create(cluster, config);

    register("system", system);
  }

  private void register(String name, Object bean) {
    applicationContext.getBeanFactory().registerSingleton(name, bean);
    LOGGER.debug("registered {}: {} to spring context", name, bean);
  }

  @SuppressWarnings("unchecked")
  private void registerActors() {
    ActorFactory actorFactory = new ActorFactory(applicationContext);
    register("actorFactory", actorFactory);

    for (String basePackage : basePackages) {
      try {
        List<Class<?>> classes = ClassUtils.getClassesWithAnnotation(basePackage, Actor.class);
        for (Class<?> clazz : classes) {
          if (!clazz.isAssignableFrom(UntypedActor.class)) {
            LOGGER.error("{} is not an UntypedActor.", clazz);
            continue;
          }
          ActorRef actor = actorFactory.create((Class<? extends UntypedActor>) clazz);
          register(clazz.getCanonicalName(), actor);

          if (clazz.isAssignableFrom(PipeActor.class)) {
            LOGGER.info("Found role: {}", clazz);
            actors.add(actor);
          }
        }
      } catch (ClassLoadingException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  private void start() {
    new ZookeeperClusterSeed((ExtendedActorSystem) system).join();

    Cluster.get(system).registerOnMemberUp(this::onStartup);

    registerOnRemoved();
  }

  private void onStartup() {
    ActorRef windtalker = system.actorOf(Props.create(Windtalker.class, predecessors), Windtalker.NAME);
    windtalker.tell(new LocalActors(role, actors), ActorRef.noSender());
    LOGGER.debug("[{}] started.", role);
  }


  private void registerOnRemoved() {
    Cluster.get(system).registerOnMemberRemoved(this::onRemoved);
  }

  private void onRemoved() {
    LOGGER.debug("{} get removed", getClass());
  }
}
