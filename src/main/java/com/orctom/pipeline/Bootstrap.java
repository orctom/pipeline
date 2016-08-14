package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.seed.ZookeeperClusterSeed;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry to bootstrap the actor
 * Created by chenhao on 8/3/16.
 */
public class Bootstrap {

  private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

  protected final ActorSystem system;

  private Bootstrap(String clusterName, String roleName) {
    Config config = loadConfig(roleName);
    system = ActorSystem.create(clusterName, config);
  }

  public static Bootstrap create(String clusterName, String roleName) {
    return new Bootstrap(clusterName, roleName);
  }

  public void start() {
    start(null);
  }

  public void start(Runnable onUpCallback) {
    new ZookeeperClusterSeed((ExtendedActorSystem) system).join();

    if (null != onUpCallback) {
      Cluster.get(system).registerOnMemberUp(onUpCallback);
    }
    registerOnRemoved();
  }

  public ActorRef createActor(String name, Class<?> clazz, Object... args) {
    return system.actorOf(Props.create(clazz, args), name);
  }

  private Config loadConfig(String roleName) {
    final Config fallback = ConfigFactory.load("default");
    final Config app = ConfigFactory.load().withFallback(fallback);
    return ConfigFactory.parseString(String.format("akka.cluster.roles = [%s]", roleName))
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
