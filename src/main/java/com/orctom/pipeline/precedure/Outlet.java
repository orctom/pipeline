package com.orctom.pipeline.precedure;

import com.orctom.pipeline.model.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Outlet extends Pipe {

  @Override
  protected final void sendToSuccessors(Message message) {
    throw new UnsupportedOperationException("Not supported operation.");
  }

  @Override
  protected final boolean isSuccessorsAvailable() {
    return false;
  }
}
