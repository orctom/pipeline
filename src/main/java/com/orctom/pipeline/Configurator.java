package com.orctom.pipeline;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Configurator {

  private Config config;

  private Configurator(String roleName) {
    final Config role = ConfigFactory.load(roleName);
    final Config ref = ConfigFactory.load("reference.conf");
    final Config app = ConfigFactory.load();
    config = ConfigFactory.parseString(String.format("akka.cluster.roles = [%s]", roleName))
        .withFallback(role)
        .withFallback(ref)
        .withFallback(app);
  }

  public static Configurator getInstance(String roleName) {
    return new Configurator(roleName);
  }

  public Config getConfig() {
    return config;
  }
}
