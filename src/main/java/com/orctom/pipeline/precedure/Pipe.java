package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.model.RemoteActors;
import com.orctom.pipeline.utils.SimpleMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static akka.cluster.ClusterEvent.CurrentClusterState;
import static akka.cluster.ClusterEvent.MemberUp;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class Pipe extends UntypedActor {

  protected Logger logger = LoggerFactory.getLogger(getClass());

  private Cluster cluster = Cluster.get(getContext().system());

  private Router successors = new Router(new RoundRobinRoutingLogic());

  protected abstract void onMessage(Message message);

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 10, TimeUnit.SECONDS);

  @Override
  public void preStart() throws Exception {
    logger.debug("Staring {}...", getSelf().path());
    cluster.subscribe(getSelf(), MemberUp.class);

    metrics.gauge("routee", new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        Iterator<Routee> it = successors.routees().iterator();
        while (it.hasNext()) {
          Routee routee = it.next();
          logger.debug("routee: {}", routee.toString());
        }
        return successors.routees().length();
      }
    });
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
    logger.trace("sending to successor {}", message);
    if (!isSuccessorsAvailable()) {
//      logger.error("No available successors, discarding...");
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
      logger.debug("Received routee candidates: {}", remoteActors.getActors());
      addSuccessorsToRoutee(remoteActors.getActors());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors = successors.removeRoutee(terminated.getActor());
      logger.warn("Routee {} terminated.", terminated.getActor().toString());
    } else if (message instanceof CurrentClusterState) {
      logger.debug("Started {}.", getSelf().path());
      started();

    } else {
      unhandled(message);
      logger.trace("Unhandled message: {}.", message);
    }
  }

  private void addSuccessorsToRoutee(Set<ActorRef> routees) {
    for (ActorRef routee : routees) {
      addSuccessorToRoutee(routee);
    }
  }

  private void addSuccessorToRoutee(ActorRef routee) {
    logger.debug("Adding as routee {}.", routee.toString());
    if (successors.routees().contains(routee)) {
      logger.debug("Already exists.");
      return;
    }

    getContext().watch(routee);
    successors = successors.addRoutee(routee);
    logger.info("Added as routee {}.", routee.toString());
  }
}
