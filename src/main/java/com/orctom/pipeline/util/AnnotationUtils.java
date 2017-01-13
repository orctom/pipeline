package com.orctom.pipeline.util;

import java.lang.annotation.*;

public abstract class AnnotationUtils {

  public static <A extends Annotation> A getMetaAnnotation(Class<?> clazz, Class<A> annotationType) {
    A a = clazz.getAnnotation(annotationType);
    if (null != a) {
      return a;
    }

    for (Annotation annotation : clazz.getAnnotations()) {
      Class<? extends Annotation> type = annotation.annotationType();

      if (isCommonAnnotations(type)) {
        continue;
      }
      a = getMetaAnnotation(type, annotationType);
      if (null != a) {
        return a;
      }
    }
    return null;
  }

  private static boolean isCommonAnnotations(Class<? extends Annotation> type) {
    return Target.class == type ||
        Retention.class == type ||
        Documented.class == type ||
        Inherited.class == type;
  }
}
