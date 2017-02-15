package com.orctom.pipeline;

import java.util.regex.Pattern;

public class Constants {

  static final String PIPELINE_NAME = "pipeline.name";
  static final String PIPELINE_HOST = "pipeline.host";
  static final String PIPELINE_PORT = "pipeline.port";
  static final String PIPELINE_ZK_URL = "pipeline.zk.url";

  static final String CFG_NAME = PIPELINE_NAME;
  static final String CFG_HOST = "akka.remote.netty.tcp.hostname";
  static final String CFG_PORT = "akka.remote.netty.tcp.port";
  static final String CFG_ZK_URL = "akka.cluster.seed.zookeeper.url";
  static final String CFG_ROLES = "akka.cluster.roles";

  public static final Pattern PATTERN_NAME = Pattern.compile("[0-9a-zA-Z-_]+");
  public static final int MAX_LEN_NAMES = 20;

  public static final String AT_SIGN = "@";
  public static final String EMPTY_STRING = "";

  public static final String Q_INBOX = "inbox";
  public static final String Q_PROCESSED = "processed";
  public static final String Q_SENT = "sent";
  public static final String Q_SENT_RECORDS = "sent_records";

  public static final String METER_INBOX = "inbox";
  public static final String METER_PROCESSED = "processed";
  public static final String METER_SENT = "sent";
  public static final String METER_RESENT = "resent";

  public static final String MEMBER_EVENT_UP = "up";
  public static final String MEMBER_EVENT_DOWN = "down";
}
