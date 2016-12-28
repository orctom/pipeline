package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import com.google.common.collect.Sets;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.util.IdUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Entry to bootstrap the actor
 * Created by chenhao on 8/3/16.
 */
public class Pipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

  protected String role;
  private final ActorSystem system;
  private Set<String> predecessors;

  private List<ActorRef> actors = new ArrayList<>();

  private Pipeline(ActorSystem system, String roleName, Set<String> predecessors) {
    this.role = roleName;
    this.system = system;
    this.predecessors = predecessors;
  }

  public static Pipeline create(String clusterName, String roleName, String... predecessors) {
    IdUtils.generate();
    Config config = Configurator.getInstance(roleName).getConfig();
    ActorSystem system = ActorSystem.create(clusterName, config);
    return new Pipeline(system, roleName, Sets.newHashSet(predecessors));
  }

  public void start() {
    start(null);
  }

  private void start(final Runnable onUpCallback) {
    new ZookeeperClusterSeed((ExtendedActorSystem) system).join();

    Cluster.get(system).registerOnMemberUp(() -> {
      if (null != onUpCallback) {
        onUpCallback.run();
      }
      onStartup();
    });

    registerOnRemoved();
  }

  private void onStartup() {
    ActorRef windtalker = system.actorOf(Props.create(Windtalker.class, predecessors), Windtalker.NAME);
    windtalker.tell(new LocalActors(role, actors), ActorRef.noSender());
    LOGGER.debug("Bootstrap started.");
  }

  public ActorRef createActor(String name, Class<?> clazz, Object... args) {
    LOGGER.debug("Creating actor: {}.", name);
    ActorRef actor = system.actorOf(Props.create(clazz, args), name);
    actors.add(actor);
    LOGGER.debug("Created  actor: {}.", name);
    return actor;
  }

  private void registerOnRemoved() {
    Cluster.get(system).registerOnMemberRemoved(this::onRemoved);
  }

  private void onRemoved() {
    LOGGER.debug("{} get removed", getClass());
  }
}
