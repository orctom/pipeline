package com.orctom.pipeline;

public class RoleCMain {

  public static void main(String[] args) {
    String cluster = "dummy";
    String role = "roleC";
    final Bootstrap bootstrap = Bootstrap.create(cluster, role, "roleB");
    bootstrap.start(new Runnable() {
      @Override
      public void run() {
        bootstrap.createActor("roleC", RoleC.class);
      }
    });
  }
}