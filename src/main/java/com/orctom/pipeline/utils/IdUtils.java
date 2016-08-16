package com.orctom.pipeline.utils;

/**
 * Unsafe GENERATOR, as it could generate same IDs on different host.
 * Created by chenhao on 8/16/16.
 */
public abstract class IdUtils {

  private static final IdGenerator GENERATOR = IdGenerator.create();

  public static long generate() {
    return GENERATOR.generate();
  }
}
