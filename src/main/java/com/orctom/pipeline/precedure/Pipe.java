package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.model.MessageAck;
import com.orctom.pipeline.model.RemoteActors;
import com.orctom.pipeline.model.Successors;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQ;
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

  private Successors successors = new Successors(getContext(), getSelf());

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 10, TimeUnit.SECONDS);

  @Override
  public void preStart() throws Exception {
    logger.debug("Staring actor: {}...", getSelf().path());

    if (logger.isTraceEnabled()) {
      logSuccessors();
    }
  }

  protected void started() {
    RMQ.getInstance().subscribe("inbox", new MessageConsumer(this));
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
    RMQ.getInstance().send("ready", message);
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof RemoteActors) { // from windtalker
      RemoteActors remoteActors = (RemoteActors) message;
      logger.debug("Received successors: {} -> {}", remoteActors.getRole(), remoteActors.getActors());
      addSuccessors(remoteActors.getRole(), remoteActors.getActors());
      started();

    } else if (message instanceof Message) {
      Message msg = (Message) message;
      RMQ.getInstance().send("inbox", msg);
      getSender().tell(new MessageAck(msg.getId()), getSelf());

    } else if (message instanceof MessageAck) {
      MessageAck msg = (MessageAck) message;
      RMQ.getInstance().delete("sent", msg.getId());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors.remove(terminated.getActor());
      logger.warn("Routee {} terminated.", terminated.getActor().toString());

    } else {
      unhandled(message);
      logger.trace("Unhandled message: {}.", message);
    }
  }

  protected abstract Ack onMessage(Message message);

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
