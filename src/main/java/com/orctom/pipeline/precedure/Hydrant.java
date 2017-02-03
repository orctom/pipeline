package com.orctom.pipeline.precedure;

import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

import static com.orctom.pipeline.Constants.Q_PROCESSED;
import static com.orctom.pipeline.Constants.Q_SENT;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Hydrant extends PipeActor implements Runnable {

  @Override
  protected final void started() {
    metrics.gauge(Q_PROCESSED + "-size", () -> "size: " + messageQueue.getSize(Q_PROCESSED));
    metrics.gauge(Q_SENT + "-size", () -> "size: " + messageQueue.getSize(Q_SENT));
    new Thread(this).start();
  }

  @Override
  protected final void subscribeInbox() {
    // do nothing, as no predecessor for a Hydrant
  }

  @Override
  public abstract void run();

  @Override
  public final Ack onMessage(Message message) {
    return Ack.DONE;
  }
}
