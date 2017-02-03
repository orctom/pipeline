package com.orctom.pipeline.precedure;

import com.orctom.rmq.Message;

import static com.orctom.pipeline.Constants.Q_INBOX;

/**
 * source of stream
 * Created by hao on 7/18/16.
 */
public abstract class Outlet extends PipeActor {

  @Override
  protected void started() {
    super.started();
    metrics.gauge(Q_INBOX + "-size", () -> "size: " + messageQueue.getSize(Q_INBOX));
  }

  @Override
  protected final void sendToSuccessors(Message message) {
    throw new UnsupportedOperationException("'sendToSuccessors()' Not supported in 'Outlet'.");
  }
}
