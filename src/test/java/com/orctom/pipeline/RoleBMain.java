package com.orctom.pipeline;

public class RoleBMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleB";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role, "roleA");
    bootstrap.createActor("roleB", RoleB.class);
    bootstrap.start();
  }
}