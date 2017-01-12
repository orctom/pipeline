package com.orctom.pipeline;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.orctom.laputa.exception.ClassLoadingException;
import com.orctom.laputa.exception.IllegalArgException;
import com.orctom.laputa.utils.ClassUtils;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.model.LocalMetricsCollectorActors;
import com.orctom.pipeline.model.Role;
import com.orctom.pipeline.precedure.AbstractMetricsCollector;
import com.orctom.pipeline.precedure.PipeActor;
import com.orctom.pipeline.util.IdUtils;
import com.orctom.pipeline.util.RoleUtils;
import com.orctom.rmq.RMQOptions;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Pipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

  private static final Pipeline INSTANCE = new Pipeline();

  private String cluster;
  private String name;
  private Set<String> roles = new HashSet<>();
  private Set<String> interestedRoles = new HashSet<>();
  private String[] basePackages;
  private AnnotationConfigApplicationContext applicationContext;
  private ActorSystem system;

  private LocalActors actors = new LocalActors();
  private LocalMetricsCollectorActors metricsCollectors = new LocalMetricsCollectorActors();

  private Pipeline() {
  }

  public static Pipeline getInstance() {
    return INSTANCE;
  }

  public Pipeline withCluster(String cluster) {
    this.cluster = cluster;
    return this;
  }

  public Pipeline withName(String name) {
    this.name = name;
    RMQOptions.getInstance().setId(name);
    return this;
  }

  public void run(Class<?> configurationClass) {
    IdUtils.generate();
    validate();
    validate(configurationClass);
    createApplicationContext(configurationClass);
    Set<Class<? extends UntypedActor>> untypedActorTypes = collectRolesFromPipeActors();
    createActorSystem();
    createActors(untypedActorTypes);
    start();
  }

  private void validate() {
    if (Strings.isNullOrEmpty(cluster)) {
      throw new IllegalArgException("`cluster` is required.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgException("`name` is required.");
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
    ActorFactory.setApplicationContext(applicationContext);
  }

  @SuppressWarnings("unchecked")
  private Set<Class<? extends UntypedActor>> collectRolesFromPipeActors() {
    Set<Class<? extends UntypedActor>> untypedActorTypes = new HashSet<>();
    for (String basePackage : basePackages) {
      try {
        List<Class<?>> classes = ClassUtils.getClassesWithAnnotation(basePackage, Actor.class);
        for (Class<?> clazz : classes) {
          if (!UntypedActor.class.isAssignableFrom(clazz)) {
            LOGGER.error("{} is not an UntypedActor.", clazz);
            continue;
          }

          untypedActorTypes.add((Class<? extends UntypedActor>) clazz);

          if (PipeActor.class.isAssignableFrom(clazz)) {
            Role role = RoleUtils.getRole(clazz);
            roles.add(role.getRole());
            interestedRoles.addAll(role.getInterestedRoles());
          }

          if (AbstractMetricsCollector.class.isAssignableFrom(clazz)) {
            roles.add(RoleUtils.getRole(clazz).getRole());
          }
        }
      } catch (ClassLoadingException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }

    return untypedActorTypes;
  }

  private void createActorSystem() {
    LOGGER.info("Bootstrapping {} with roles of {}", name, roles);
    Config config = Configurator.getInstance(name, Joiner.on(',').join(roles)).getConfig();
    system = ActorSystem.create(cluster, config);

    register("system", system);
  }

  private void register(String name, Object bean) {
    applicationContext.getBeanFactory().registerSingleton(name, bean);
    LOGGER.debug("registered {}: {} to spring context", name, bean.getClass());
  }

  private void createActors(Set<Class<? extends UntypedActor>> untypedActorTypes) {
    for (Class<? extends UntypedActor> actorType : untypedActorTypes) {
      // start the actor
      ActorRef actor = ActorFactory.actorOf(actorType);

      if (PipeActor.class.isAssignableFrom(actorType)) {
        LOGGER.info("Found pipeline actor: {}", actorType);
        actors.addActor(actor, RoleUtils.getRole(actorType));
      }

      if (AbstractMetricsCollector.class.isAssignableFrom(actorType)) {
        LOGGER.info("Found MetricsCenterActor: {}", actorType);
        metricsCollectors.add(actor);
      }
    }
  }

  private void start() {
    new ZookeeperClusterSeed((ExtendedActorSystem) system).join();

    Cluster.get(system).registerOnMemberUp(this::onStartup);

    registerOnRemoved();
  }

  private void onStartup() {
    ActorRef windtalker = system.actorOf(Props.create(Windtalker.class, interestedRoles), Windtalker.NAME);
    windtalker.tell(actors, ActorRef.noSender());
    if (!metricsCollectors.getActors().isEmpty()) {
      windtalker.tell(metricsCollectors, ActorRef.noSender());
    }
    LOGGER.debug("[{}] started.", name);
  }

  private void registerOnRemoved() {
    Cluster.get(system).registerOnMemberRemoved(this::onRemoved);
  }

  private void onRemoved() {
    LOGGER.debug("{} get removed", getClass());
  }

  public ApplicationContext getApplicationContext() {
    return applicationContext;
  }

  public ActorSystem getSystem() {
    return system;
  }

  public String getCluster() {
    return cluster;
  }

  public String getName() {
    return name;
  }

  public Set<String> getRoles() {
    return roles;
  }
}
