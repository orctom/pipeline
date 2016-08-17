package com.orctom.pipeline.model;

/**
 * A cache
 * Created by chenhao on 8/17/16.
 */
public class MessageCache<T, V> {

  private T destination;
  private V message;

  public MessageCache(T destination, V message) {
    this.destination = destination;
    this.message = message;
  }

  public T getDestination() {
    return destination;
  }

  public V getMessage() {
    return message;
  }
}
