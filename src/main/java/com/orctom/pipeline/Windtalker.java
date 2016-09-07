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
import com.orctom.pipeline.model.MessageAck;
import com.orctom.pipeline.model.MessageCache;
import com.orctom.pipeline.model.RemoteActors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

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

  private Map<Long, MessageCache<ActorSelection, RemoteActors>> windtalkerMessages = new ConcurrentHashMap<>();
  private ScheduledFuture<?> scheduled;

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

    startResendingThread();
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
      LOGGER.debug("Got a list of local actors: {}.", localActors.getActors());

    } else if (message instanceof RemoteActors) { // from successors
      RemoteActors remoteActors = (RemoteActors) message;
      LOGGER.debug("Got a list of remote actors: {}.", remoteActors.getActors());
      informLocalActors(remoteActors);
      getSender().tell(new MessageAck(remoteActors), getSelf());

    } else if (message instanceof MessageAck) { // from successors
      LOGGER.debug("Got ack from predecessor.");
      MessageAck ack = (MessageAck) message;
      windtalkerMessages.remove(ack.getId());

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
    LOGGER.debug(" Informing local actors: {}.", localActors.getActors());
    if (null == localActors || null == localActors.getActors() || localActors.getActors().isEmpty()) {
      LOGGER.error("No user actors started.");
      return;
    }

    for (ActorRef localActor : localActors.getActors()) {
      localActor.tell(remoteActors, getSelf());
      LOGGER.debug("  Informed : {}.", localActor);
    }
  }

  private void registerMember(Member member) {
    LOGGER.debug(" Registering member {}, with roles: {}.", member.address(), member.getRoles());
    for (String role : predecessors) {
      if (member.hasRole(role)) {
        notifyPredecessor(member);
      }
    }
  }

  private void notifyPredecessor(Member member) {
    String predecessorWindtalkerAddress = member.address() + "/user/" + NAME;
    ActorSelection predecessor = getContext().actorSelection(predecessorWindtalkerAddress);
    RemoteActors remoteActors = new RemoteActors(localActors.getRole(), localActors.getActors());

    predecessor.tell(remoteActors, getSelf());
    windtalkerMessages.put(remoteActors.getId(), new MessageCache<>(predecessor, remoteActors));

    LOGGER.debug("  Notified predecessor windtalker {}.", predecessorWindtalkerAddress);
  }

  private void startResendingThread() {
    LOGGER.debug("Started resending thread.");
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduled = scheduler.scheduleWithFixedDelay(new Runnable() {

      int maxEmptyLoop = 3;

      @Override
      public void run() {
        int emptyLoop = 0;
        if (windtalkerMessages.isEmpty()) {
          if (++emptyLoop >= maxEmptyLoop) {
            stopResendingThread();
          }
          return;
        }
        LOGGER.debug("Resending un-acked message, remains {}.", windtalkerMessages.size());
        for (MessageCache<ActorSelection, RemoteActors> messageCache : windtalkerMessages.values()) {
          messageCache.getDestination().tell(messageCache.getMessage(), getSelf());
        }
      }
    }, 3, 3, TimeUnit.SECONDS);
  }

  private void stopResendingThread() {
    LOGGER.debug("Stopped resending thread.");
    scheduled.cancel(true);
    scheduled = null;
  }
}
