package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.model.RemoteActors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Represents a node in the cluster.
 * Created by hao on 8/15/16.
 */
public class Windtalker extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Windtalker.class);

  public static final String NAME = "windtalker";

  private Cluster cluster = Cluster.get(getContext().system());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private LocalActors localActors;

  protected Set<String> predecessors;

  public Windtalker(Set<String> predecessors) {
    this.predecessors = predecessors;
  }

  @Override
  public void preStart() throws Exception {
    LOGGER.debug("Staring Windtalker...");
    if (hasPredecessors()) {
      LOGGER.debug("{} -> this.", predecessors);
      cluster.subscribe(getSelf(), MemberUp.class);
    } else {
      LOGGER.debug("Started (Hydrant).");
    }
  }

  @Override
  public void postStop() throws Exception {
    cluster.unsubscribe(getSelf());
  }

  protected boolean hasPredecessors() {
    return null != predecessors && !predecessors.isEmpty();
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof LocalActors) {
      localActors = (LocalActors) message;
      LOGGER.debug("Get a list of local actors: {}.", localActors.getActors());

    } else if (message instanceof RemoteActors) {
      RemoteActors remoteActors = (RemoteActors) message;
      LOGGER.debug("Get a list of remote actors: {}.", remoteActors.getActors());
      informLocalActors(remoteActors);

    } else if (message instanceof CurrentClusterState) {
      LOGGER.trace("processing CurrentClusterState event.");
      CurrentClusterState state = (CurrentClusterState) message;
      for (Member member : state.getMembers()) {
        if (member.status().equals(MemberStatus.up())) {
          LOGGER.trace(" Live member: {}.", member.toString());
          registerMember(member);
        }
      }

    } else if (message instanceof MemberUp) {
      MemberUp mUp = (MemberUp) message;
      LOGGER.trace("Member {} is up.", mUp.member().toString());
      registerMember(mUp.member());

    } else {
      unhandled(message);
      LOGGER.trace("Unhandled message: {}.", message);
    }
  }

  private void informLocalActors(RemoteActors remoteActors) {
    if (null == localActors || null == localActors.getActors() || localActors.getActors().isEmpty()) {
      LOGGER.error("No user actors started.");
      return;
    }

    for (ActorRef localActor : localActors.getActors()) {
      localActor.tell(remoteActors, getSelf());
    }
  }

  private void registerMember(Member member) {
    LOGGER.trace(" Registering member {}, with roles: {}.", member.address(), member.getRoles());
    for (String role : predecessors) {
      if (member.hasRole(role)) {
        notifyPredecessor(member);
      }
    }
  }

  private void notifyPredecessor(Member member) {
    String predecessorWindtalkerAddress = member.address() + "/user/" + NAME;
    final ActorSelection predecessor = getContext().actorSelection(predecessorWindtalkerAddress);
    final RemoteActors remoteActors = new RemoteActors(localActors.getActors());

    predecessor.tell(remoteActors, getSelf());

    LOGGER.debug("  Notified predecessor windtalker {}.", predecessorWindtalkerAddress);
  }

  private void scheduledCall(final ActorSelection predecessor, final RemoteActors remoteActors) {
    final ScheduledFuture<?> notifyHandle = scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        predecessor.tell(remoteActors, getSelf());
      }
    }, 0, 3, TimeUnit.SECONDS);

    scheduler.schedule(new Runnable() {
      public void run() {
        notifyHandle.cancel(true);
      }
    }, 10, TimeUnit.SECONDS);
  }
}
