package com.orctom.pipeline.sample;

import com.orctom.pipeline.Bootstrap;

public class RoleAMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleA";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role);
    bootstrap.createActor("roleA", RoleA.class);
    bootstrap.start();
  }
}