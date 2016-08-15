package com.orctom.pipeline.precedure;

import com.google.common.base.Strings;
import com.orctom.pipeline.model.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Hydrant extends AbstractProcedure {

  @Override
  protected final String getPredecessorRoleName() {
    return null;
  }

  @Override
  protected void beforeStart() {
    run();
  }

  protected abstract void run();

  @Override
  protected void onMessage(Message message) {
  }

  @Override
  protected void validate() {
    if (Strings.isNullOrEmpty(getSuccessorRoleName())) {
      throw new IllegalArgumentException("getSuccessorRoleName() not providing validate value");
    }
  }
}
