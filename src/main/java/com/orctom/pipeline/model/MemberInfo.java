package com.orctom.pipeline.model;

public class MemberInfo extends PipelineMessage {

  private String applicationName;
  private String roles;

  public MemberInfo() {
  }

  public MemberInfo(String applicationName, String roles) {
    this.applicationName = applicationName;
    this.roles = roles;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getRoles() {
    return roles;
  }

  public void setRoles(String roles) {
    this.roles = roles;
  }

  @Override
  public String toString() {
    return "MemberEvent{" +
        "applicationName='" + applicationName + '\'' +
        ", roles='" + roles + '\'' +
        "} " + super.toString();
  }
}
