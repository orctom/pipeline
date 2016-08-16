package com.orctom.pipeline.precedure;

import com.orctom.pipeline.model.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Hydrant extends Pipe {

  @Override
  public void preStart() throws Exception {
    super.preStart();
    run();
  }

  protected abstract void run();

  @Override
  protected void onMessage(Message message) {
  }
}
