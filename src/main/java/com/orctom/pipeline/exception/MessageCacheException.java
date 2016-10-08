package com.orctom.pipeline.exception;

public class MessageCacheException extends FastException {
  
  public MessageCacheException(String message) {
    super(message);
  }

  public MessageCacheException(String message, Throwable cause) {
    super(message, cause);
  }
}
