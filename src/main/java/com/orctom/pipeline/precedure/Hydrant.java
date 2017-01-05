package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Hydrant extends PipeActor {

  @Override
  public final void preStart() throws Exception {
    super.preStart();
    run();
  }

  protected abstract void run();

  @Override
  protected final Ack onMessage(Message message) {
    return Ack.DONE;
  }
}
