package com.orctom.pipeline.util;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.orctom.laputa.exception.IllegalConfigException;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.Role;

import java.util.HashMap;
import java.util.Map;

public abstract class RoleUtils {

  private static final Map<Class<?>, Role> ROLES = new HashMap<>();

  public static Role getRole(Class<?> clazz) {
    return ROLES.computeIfAbsent(clazz, RoleUtils::getRoleFromAnnotation);
  }

  private static Role getRoleFromAnnotation(Class<?> clazz) {
    Actor actor = clazz.getAnnotation(Actor.class);
    if (null == actor) {
      throw new IllegalConfigException("Annotation: 'Actor' expected but not found on class: " + clazz);
    }

    String role = actor.role();
    if (Strings.isNullOrEmpty(role)) {
      role = actor.value();
      if (Strings.isNullOrEmpty(role)) {
        throw new IllegalConfigException("'value' in 'Actor' expected but not set. on class: " + clazz);
      }
    }

    String[] interestedRoles = actor.interestedRoles();
    return new Role(role, Sets.newHashSet(interestedRoles));
  }
}
