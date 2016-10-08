package com.orctom.pipeline;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Configurator {

  private static Configurator configurator;

  private Config config;

  private Configurator(String roleName) {
    final Config role = ConfigFactory.load(roleName);
    final Config app = ConfigFactory.load();
    config = ConfigFactory.parseString(String.format("akka.cluster.roles = [%s]", roleName))
        .withFallback(role)
        .withFallback(app);
  }

  static void init(String roleName) {
    Configurator.configurator = new Configurator(roleName);
  }

  public static Configurator getInstance() {
    return configurator;
  }

  public Config getConfig() {
    return config;
  }
}
