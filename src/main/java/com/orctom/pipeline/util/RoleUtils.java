package com.orctom.pipeline.util;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.orctom.laputa.exception.IllegalConfigException;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.Role;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.precedure.Pipe;

import java.util.HashMap;
import java.util.Map;

import static com.orctom.pipeline.Constants.MAX_LEN_NAMES;
import static com.orctom.pipeline.Constants.PATTERN_NAME;

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
      validateRole(clazz, role);
    }

    String[] interestedRoles = actor.interestedRoles();
//    validateInterestedRoles(clazz, interestedRoles);
    return new Role(role, Sets.newHashSet(interestedRoles));
  }

  private static void validateRole(Class<?> clazz, String role) {
    if (Strings.isNullOrEmpty(role)) {
      throw new IllegalConfigException("'value' in 'Actor' expected but not set. on class: " + clazz);
    }
    if (role.length() > MAX_LEN_NAMES) {
      throw new IllegalConfigException("'value' in 'Actor' should not be longer than 20,  value: " + role);
    }
    if (!PATTERN_NAME.matcher(role).matches()) {
      throw new IllegalConfigException("Illegal role: " + role + ", only allows '0-9', 'a-z', 'A-Z', '-' and '_'");
    }
  }

  private static void validateInterestedRoles(Class<?> clazz, String[] interestedRoles) {
    if ((null == interestedRoles || 0 == interestedRoles.length) &&
        (Pipe.class.isAssignableFrom(clazz) || Outlet.class.isAssignableFrom(clazz))) {
      throw new IllegalConfigException("interestedRoles expected for: " + clazz.getName());
    }
  }
}
