package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Router;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.model.RemoteActors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static akka.cluster.ClusterEvent.*;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class Pipe extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Pipe.class);

  private Cluster cluster = Cluster.get(getContext().system());

  private Router successors = new Router(new RoundRobinRoutingLogic());

  protected abstract void onMessage(Message message);

  @Override
  public void preStart() throws Exception {
    LOGGER.debug("Staring {}...", getSelf().path());
    cluster.subscribe(getSelf(), MemberUp.class);
  }

  @Override
  public void postStop() throws Exception {
    cluster.unsubscribe(getSelf());
  }

  protected boolean isSuccessorsAvailable() {
    return !successors.routees().isEmpty();
  }

  protected void started() {
  }

  protected void sendToSuccessor(Message message) {
    LOGGER.trace("sending to successor {}", message);
    if (!isSuccessorsAvailable()) {
//      LOGGER.error("No available successors, discarding...");
      return;
    }
    successors.route(message, getSelf());
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof Message) {
      onMessage((Message) message);

    } else if (message instanceof RemoteActors) {
      RemoteActors remoteActors = (RemoteActors) message;
      LOGGER.trace("Received actors of successors: {}", remoteActors.getActors());
      addSuccessorsToRoutee(remoteActors.getActors());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors = successors.removeRoutee(terminated.getActor());
      LOGGER.trace("Successor {} terminated.", terminated.getActor().toString());
    } else if (message instanceof CurrentClusterState) {
      LOGGER.debug("Started {}.", getSelf().path());
      started();

    } else {
      unhandled(message);
      LOGGER.trace("Unhandled message: {}.", message);
    }
  }

  private void addSuccessorsToRoutee(Set<ActorRef> routees) {
    for (ActorRef routee : routees) {
      addSuccessorToRoutee(routee);
    }
  }

  private void addSuccessorToRoutee(ActorRef routee) {
    LOGGER.trace("Adding as routee {}.", routee.toString());
    if (successors.routees().contains(routee)) {
      LOGGER.trace(" Already exists.");
      return;
    }

    getContext().watch(routee);
    successors = successors.addRoutee(routee);
    LOGGER.trace(" Added.");
  }
}
