package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Hydrant extends PipeActor implements Runnable {

  @Override
  protected final void started() {
    new Thread(this).start();
    System.out.println("started thread");
  }

  @Override
  protected final void subscribeInbox() {
    // do nothing, as no predecessor for a Hydrant
  }

  @Override
  public abstract void run();

  @Override
  protected final Ack onMessage(Message message) {
    return Ack.DONE;
  }
}
