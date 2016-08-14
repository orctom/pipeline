package com.orctom.pipeline;

public class RoleAMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleA";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role);
    bootstrap.start(new Runnable() {
      @Override
      public void run() {
        bootstrap.createActor("roleAHydrant", RoleA.class);
      }
    });
  }
}