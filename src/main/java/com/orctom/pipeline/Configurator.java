package com.orctom.pipeline;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.orctom.laputa.exception.IllegalConfigException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.orctom.pipeline.Constants.*;

public class Configurator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Configurator.class);

  private Config config;

  private Configurator(String applicationName, String roles) {
    final Config node = ConfigFactory.load(applicationName);
    Map<String, Object> pipeline = new HashMap<>();
    config(node, PIPELINE_NAME, pipeline, CFG_NAME);
    config(node, PIPELINE_HOST, pipeline, CFG_HOST);
    config(node, PIPELINE_PORT, pipeline, CFG_PORT);
    config(node, PIPELINE_ZK_URL, pipeline, CFG_ZK_URL);
    config(node, PIPELINE_THROTTLE, pipeline, CFG_THROTTLE, DEFAULT_THROTTLE);
    pipeline.put(CFG_ROLES, Splitter.on(",").omitEmptyStrings().trimResults().splitToList(roles));

    config = ConfigFactory.parseMap(pipeline)
        .withFallback(node)
        .withFallback(ConfigFactory.load());
  }

  private void config(Config node, String property, Map<String, Object> pipeline, String key) {
    config(node, property, pipeline, key, null);
  }

  private void config(Config node, String property, Map<String, Object> pipeline, String key, String defaultValue) {
    String value = getConfig(node, property, defaultValue);
    if (Strings.isNullOrEmpty(value)) {
      throw new IllegalConfigException(property + " expected, but is null or empty");
    }

    LOGGER.info("{}: {}", property, value);
    pipeline.put(key, value);
  }

  private String getConfig(Config conf, String property, String defaultValue) {
    String value = System.getProperty(property);
    if (!Strings.isNullOrEmpty(value)) {
      return value;
    }
    if (conf.hasPath(property)) {
      return conf.getString(property);
    }
    return defaultValue;
  }

  public static Configurator getInstance(String applicationName, String roles) {
    if (Strings.isNullOrEmpty(applicationName)) {
      throw new IllegalArgumentException("Node name is empty, call 'withApplicationName(...)' on Pipeline to have it set.");
    }
    if (Strings.isNullOrEmpty(roles)) {
      throw new IllegalArgumentException("No actors found with annotation 'Actor' and which attribute 'role' is set.");
    }

    return new Configurator(applicationName, roles);
  }

  public Config getConfig() {
    return config;
  }
}
