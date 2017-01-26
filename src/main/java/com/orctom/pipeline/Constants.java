package com.orctom.pipeline;

import java.util.regex.Pattern;

public class Constants {

  public static final Pattern PATTERN_NAME = Pattern.compile("[0-9a-zA-Z-_]+");
  public static final int MAX_LEN_NAMES = 20;

  public static final String Q_INBOX = "inbox";
  public static final String Q_READY = "ready";
  public static final String Q_SENT = "sent";

  public static final String METER_INBOX = "inbox";
  public static final String METER_READY = "ready";
  public static final String METER_SENT = "sent";

  public static final String MEMBER_EVENT_UP = "up";
  public static final String MEMBER_EVENT_DOWN = "down";
}
