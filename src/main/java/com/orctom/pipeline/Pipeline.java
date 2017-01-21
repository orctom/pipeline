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
import com.orctom.pipeline.util.AnnotationUtils;
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
import org.springframework.context.support.GenericApplicationContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class Pipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

  private static final Pipeline INSTANCE = new Pipeline();

  private static final Pattern PATTERN_NAME = Pattern.compile("[0-9a-zA-Z-_]+");

  private String cluster;
  private String applicationName;
  private Set<String> roles = new HashSet<>();
  private Set<String> interestedRoles = new HashSet<>();
  private String[] basePackages;
  private ApplicationContext applicationContext;
  private ActorSystem system;

  private LocalActors actors = new LocalActors();
  private LocalMetricsCollectorActors metricsCollectors = new LocalMetricsCollectorActors();

  private Pipeline() {
  }

  public static Pipeline getInstance() {
    return INSTANCE;
  }

  public Pipeline withCluster(String cluster) {
    validateName(cluster);
    this.cluster = cluster;
    return this;
  }

  public Pipeline withApplicationName(String applicationName) {
    validateName(applicationName);
    this.applicationName = applicationName;
    return this;
  }

  private void validateName(String name) {
    if (!PATTERN_NAME.matcher(name).matches()) {
      throw new IllegalArgException("Illegal name: " + name + ", only allows '0-9', 'a-z', 'A-Z', '-' and '_'");
    }
  }

  public Pipeline withApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
    return this;
  }

  public void run(Class<?> configurationClass) {
    IdUtils.generate();
    validate();
    validate(configurationClass);
    createApplicationContextIfNotSet(configurationClass);
    Set<Class<? extends UntypedActor>> untypedActorTypes = collectRolesFromPipeActors();
    createActorSystem();
    createActors(untypedActorTypes);
    start();
  }

  private void validate() {
    if (Strings.isNullOrEmpty(cluster)) {
      throw new IllegalArgException("`cluster` is required.");
    }
    if (Strings.isNullOrEmpty(applicationName)) {
      throw new IllegalArgException("`applicationName` is required.");
    }
  }

  private void validate(Class<?> configurationClass) {
    if (null == configurationClass) {
      throw new IllegalArgException("Null class to 'run()'!");
    }
    validateIfConfigurationPresent(configurationClass);
    validateAndRetrieveBasePackages(configurationClass);
  }

  private void validateIfConfigurationPresent(Class<?> configurationClass) {
    Configuration configuration = AnnotationUtils.getMetaAnnotation(configurationClass, Configuration.class);
    if (null == configuration) {
      throw new IllegalArgException("@Configuration is expected on class: " + configurationClass);
    }
  }

  private void validateAndRetrieveBasePackages(Class<?> configurationClass) {
    ComponentScan componentScan = AnnotationUtils.getMetaAnnotation(configurationClass, ComponentScan.class);
    if (null == componentScan) {
      throw new IllegalArgException("@ComponentScan is expected on class: " + configurationClass);
    } else {
      basePackages = componentScan.basePackages();
      if (0 == basePackages.length) {
        basePackages = new String[]{configurationClass.getPackage().getName()};
      }
      LOGGER.info("basePackages: {}", Arrays.toString(basePackages));
    }
  }

  private void createApplicationContextIfNotSet(Class<?> configurationClass) {
    if (null == applicationContext) {
      applicationContext = new AnnotationConfigApplicationContext(configurationClass);
    }

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
    LOGGER.info("Bootstrapping {} with roles of {}", applicationName, roles);
    Config config = Configurator.getInstance(applicationName, Joiner.on(',').join(roles)).getConfig();
    system = ActorSystem.create(cluster, config);

    GenericApplicationContext context = ((GenericApplicationContext) applicationContext);
    context.getBeanFactory().registerSingleton("system", system);
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
    LOGGER.debug("[{}] started.", applicationName);
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

  public String getApplicationName() {
    return applicationName;
  }

  public Set<String> getRoles() {
    return roles;
  }
}
