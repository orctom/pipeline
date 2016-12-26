package com.orctom.pipeline.sample;

import com.orctom.pipeline.Bootstrap;

public class RoleCMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleC";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role, "roleB");
    bootstrap.createActor("roleC", RoleC.class);
    bootstrap.start();
  }
}