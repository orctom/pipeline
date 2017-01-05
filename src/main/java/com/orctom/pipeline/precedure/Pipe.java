package com.orctom.pipeline.precedure;

import com.orctom.rmq.Message;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class Pipe extends AbstractPipe {

  protected final void sendToSuccessors(Message message) {
    super.sendToSuccessors(message);
  }

}
