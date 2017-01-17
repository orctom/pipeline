package com.orctom.pipeline.annotation;

import org.springframework.context.annotation.Scope;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Scope("prototype")
@Component
public @interface Actor {

  @AliasFor("value")
  String role() default "";

  @AliasFor("role")
  String value() default "";

  String[] interestedRoles() default {};
}