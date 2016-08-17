package com.orctom.pipeline;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import com.google.common.collect.Sets;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.utils.IdUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Entry to bootstrap the actor
 * Created by chenhao on 8/3/16.
 */
public class Bootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

  protected final ActorSystem system;
  protected ActorRef windtalker;
  protected Set<String> predecessors;

  private Set<ActorRef> actors = new HashSet<>();

  private Bootstrap(String clusterName, String roleName, Set<String> predecessors) {
    Config config = loadConfig(roleName);
    system = ActorSystem.create(clusterName, config);
    this.predecessors = predecessors;
  }

  public static Bootstrap create(String clusterName, String roleName, String... predecessors) {
    IdUtils.generate();
    return new Bootstrap(clusterName, roleName, Sets.newHashSet(predecessors));
  }

  public void start() {
    start(null);
  }

  public void start(final Runnable onUpCallback) {
    new ZookeeperClusterSeed((ExtendedActorSystem) system).join();

    Cluster.get(system).registerOnMemberUp(new Runnable() {
      @Override
      public void run() {
        if (null != onUpCallback) {
          onUpCallback.run();
        }
        onStartup();
      }
    });

    registerOnRemoved();
  }

  private void onStartup() {
    windtalker = system.actorOf(Props.create(Windtalker.class, predecessors), Windtalker.NAME);
    windtalker.tell(new LocalActors(actors), ActorRef.noSender());
    LOGGER.debug("Bootstrap started.");
  }

  public ActorRef createActor(String name, Class<?> clazz, Object... args) {
    LOGGER.debug("Creating actor: {}.", name);
    ActorRef actor = system.actorOf(Props.create(clazz, args), name);
    actors.add(actor);
    LOGGER.debug("Created  actor: {}.", name);
    return actor;
  }

  private Config loadConfig(String roleName) {
    final Config role = ConfigFactory.load(roleName);
    final Config app = ConfigFactory.load();
    return ConfigFactory.parseString(String.format("akka.cluster.roles = [%s]", roleName))
        .withFallback(role)
        .withFallback(app);
  }

  private void registerOnRemoved() {
    Cluster.get(system).registerOnMemberRemoved(new Runnable() {
      @Override
      public void run() {
        onRemoved();
      }
    });
  }

  protected void onRemoved() {
    LOGGER.debug("{} get removed", getClass());
  }
}
