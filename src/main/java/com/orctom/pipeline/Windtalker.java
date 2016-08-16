package com.orctom.pipeline;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import com.orctom.pipeline.model.LocalActors;
import com.orctom.pipeline.model.RemoteActors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.Set;

/**
 * Represents a node in the cluster.
 * Created by hao on 8/15/16.
 */
public class Windtalker extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Windtalker.class);

  public static final String NAME = "windtalker";

  private Cluster cluster = Cluster.get(getContext().system());

  private LocalActors localActors;

  protected Set<String> predecessors;

  public Windtalker(Set<String> predecessors) {
    this.predecessors = predecessors;
  }

  @Override
  public void preStart() throws Exception {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Staring Windtalker...");
      if (hasPredecessors()) {
        LOGGER.trace("{} -> this.", predecessors);
        cluster.subscribe(getSelf(), MemberUp.class);
      } else {
        LOGGER.trace("Started (Hydrant).");
      }
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
      LOGGER.trace("Get a list of local actors: {}.", localActors.getActors());

    } else if (message instanceof RemoteActors) {
      RemoteActors remoteActors = (RemoteActors) message;
      LOGGER.trace("Get a list of remote actors: {}.", remoteActors.getActors());
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
    ActorSelection predecessor = getContext().actorSelection(predecessorWindtalkerAddress);
    RemoteActors remoteActors = new RemoteActors(localActors.getActors());
//    getContext().system().scheduler().schedule(
//        Duration.Zero(), Duration.create(1, "second"), getSelf(), Do,
//        getContext().dispatcher(), null
//    );
    predecessor.tell(remoteActors, getSelf());
    LOGGER.trace("  Notified predecessor windtalker {}.", predecessorWindtalkerAddress);
  }
}
