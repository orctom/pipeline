package com.orctom.pipeline;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Configurator {

  private Config config;

  private Configurator(String nodeName, String roles) {
    final Config node = ConfigFactory.load(nodeName);
    final Config ref = ConfigFactory.load("reference.conf");
    final Config app = ConfigFactory.load();
    config = ConfigFactory.parseString(String.format("akka.cluster.roles = [%s]", roles))
        .withFallback(node)
        .withFallback(ref)
        .withFallback(app);
  }

  public static Configurator getInstance(String nodeName, String roles) {
    if (Strings.isNullOrEmpty(nodeName)) {
      throw new IllegalArgumentException("'nodeName' is expected but is null or empty.");
    }
    if (Strings.isNullOrEmpty(roles)) {
      throw new IllegalArgumentException("'roles' is expected but is null or empty.");
    }

    return new Configurator(nodeName, roles);
  }

  public Config getConfig() {
    return config;
  }
}
