package com.orctom.pipeline.precedure;

import com.google.common.base.Strings;
import com.orctom.pipeline.model.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Outlet extends AbstractProcedure {

  @Override
  protected final void sendToSuccessor(Message message) {
    throw new UnsupportedOperationException("Not supported operation.");
  }

  @Override
  protected final boolean isSuccessorsAvailable() {
    return false;
  }

  @Override
  protected final String getSuccessorRoleName() {
    return null;
  }

  @Override
  protected void validate() {
    if (Strings.isNullOrEmpty(getPredecessorRoleName())) {
      throw new IllegalArgumentException("getPredecessorRoleName() not providing validate value");
    }
  }
}
