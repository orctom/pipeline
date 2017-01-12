package com.orctom.pipeline.model;

import java.util.HashSet;
import java.util.Set;

public class Role {

  private String role;
  private Set<String> interestedRoles = new HashSet<>();

  public Role(String role, Set<String> interestedRoles) {
    this.role = role;
    this.interestedRoles = interestedRoles;
  }

  public String getRole() {
    return role;
  }

  public Set<String> getInterestedRoles() {
    return interestedRoles;
  }
}
