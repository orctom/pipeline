package com.orctom.pipeline.precedure;

import com.orctom.rmq.Message;

import static com.orctom.pipeline.Constants.*;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class Pipe extends PipeActor {

  @Override
  protected void started() {
    super.started();

    metrics.gauge(Q_INBOX + "-size", () -> "size: " + messageQueue.getSize(Q_INBOX));
    metrics.gauge(Q_PROCESSED + "-size", () -> "size: " + messageQueue.getSize(Q_PROCESSED));
    metrics.gauge(Q_SENT + "-size", () -> "size: " + messageQueue.getSize(Q_SENT));
  }

  protected final void sendToSuccessors(Message message) {
    super.sendToSuccessors(message);
  }

}
