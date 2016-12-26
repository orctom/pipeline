package com.orctom.pipeline.sample;

import com.orctom.pipeline.Bootstrap;

public class RoleBMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleB";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role, "roleA");
    bootstrap.createActor("roleB", RoleB.class);
    bootstrap.start();
  }
}