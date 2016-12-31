package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.model.GroupSuccessors;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.model.RemoteActors;
import com.orctom.pipeline.model.Successors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class Pipe extends UntypedActor {

  protected Logger logger = LoggerFactory.getLogger(getClass());

  private Successors successors = new Successors(getContext());

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 10, TimeUnit.SECONDS);

  @Override
  public void preStart() throws Exception {
    logger.debug("Staring actor: {}...", getSelf().path());

    if (logger.isTraceEnabled()) {
      logSuccessors();
    }
  }

  private boolean isSuccessorsAvailable() {
    return !successors.isEmpty();
  }

  protected void started() {
  }

  private void logSuccessors() {
    metrics.gauge("routee", () -> {
      if (logger.isTraceEnabled()) {
        logger.trace(successors.toString());
      }
      return successors.size();
    });
  }

  protected void sendToSuccessors(Message message) {
    logger.trace("sending to successor {}", message);
    if (!isSuccessorsAvailable()) {
      logger.debug("No available successors, discarding...");
      return;
    }
    for (GroupSuccessors groupSuccessors : successors.getGroups()) {
      groupSuccessors.sendMessage(message, getSelf());
    }
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof RemoteActors) { // from windtalker
      RemoteActors remoteActors = (RemoteActors) message;
      logger.debug("Received successors: {} -> {}", remoteActors.getRole(), remoteActors.getActors());
      addSuccessors(remoteActors.getRole(), remoteActors.getActors());
      started();

    } else if (message instanceof Message) {
      onMessage((Message) message);

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors.remove(terminated.getActor());
      logger.warn("Routee {} terminated.", terminated.getActor().toString());

    } else {
      unhandled(message);
      logger.trace("Unhandled message: {}.", message);
    }
  }

  protected abstract void onMessage(Message message);

  private void addSuccessors(String role, List<ActorRef> actorRefs) {
    for (ActorRef actorRef : actorRefs) {
      addSuccessor(role, actorRef);
    }
  }

  private void addSuccessor(String role, ActorRef actorRef) {
    logger.debug("Adding as routee {}.", actorRef.toString());
    if (successors.addSuccessor(role, actorRef)) {
      getContext().watch(actorRef);
    } else {
      logger.debug("Already exists.");
    }
  }
}
