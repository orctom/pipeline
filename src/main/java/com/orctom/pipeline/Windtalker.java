package com.orctom.pipeline;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.orctom.laputa.utils.MutableInt;
import com.orctom.pipeline.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Represents a node in the cluster.
 * Created by hao on 8/15/16.
 */
class Windtalker extends UntypedActor {

  static final String NAME = "windtalker";

  private static final Logger LOGGER = LoggerFactory.getLogger(Windtalker.class);

  private Cluster cluster = Cluster.get(getContext().system());

  private LocalActors localActors;
  private Queue<Member> predecessorMembers = new LinkedList<>();

  private List<ActorRef> metricsCollectorActors;
  private List<Member> allMembers = new ArrayList<>();

  private Set<String> interestedRoles;

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("windtalker-resending-%d").build()
  );

  private Map<String, MessageCache<ActorSelection, PipelineMessage>> windtalkerMessages = new ConcurrentHashMap<>();
  private ScheduledFuture<?> scheduled;

  Windtalker(Set<String> interestedRoles) {
    this.interestedRoles = interestedRoles;
  }

  @Override
  public void preStart() throws Exception {
    LOGGER.debug("Staring Windtalker... {}", hasPredecessors() ? (interestedRoles + " -> this") : "as [Hydrant]");

    cluster.subscribe(getSelf(), MemberUp.class);
    startResendingThread();
  }

  @Override
  public void postStop() throws Exception {
    cluster.unsubscribe(getSelf());
  }

  private boolean hasPredecessors() {
    return null != interestedRoles && !interestedRoles.isEmpty();
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof LocalActors) { // from Pipeline
      localActors = (LocalActors) message;
      LOGGER.debug("Got a list of local actors: {}.", localActors.getActors());
      notifyPredecessorsIfExist();

    } else if (message instanceof RemoteActors) { // from successors windtalker
      RemoteActors remoteActors = (RemoteActors) message;
      LOGGER.debug("Got a list of remote actors: {}.", remoteActors.getActors());
      informLocalActors(remoteActors);
      getSender().tell(new MessageAck(remoteActors.getId()), getSelf());

    } else if (message instanceof LocalMetricsCollectorActors) { // from Pipeline
      metricsCollectorActors = ((LocalMetricsCollectorActors) message).getActors();
      LOGGER.debug("Got a list of LocalMetricsCenterActors: {}.", metricsCollectorActors);
      notifyAllMembers();

    } else if (message instanceof RemoteMetricsCollectorActors) { // from remote windtalker
      RemoteMetricsCollectorActors remoteMetricsCollectorActors = (RemoteMetricsCollectorActors) message;
      LOGGER.debug("Got a list of RemoteMetricsCenterActors: {}.", remoteMetricsCollectorActors.getActors());
      informLocalActors(remoteMetricsCollectorActors);
      getSender().tell(new MessageAck(remoteMetricsCollectorActors.getId()), getSelf());
      notifyMetricsCollectorsWithMemberInfo(getSender());

    } else if (message instanceof MessageAck) { // from successors windtalker
      LOGGER.debug("Got ack from predecessor: {}.", getSender());
      MessageAck ack = (MessageAck) message;
      windtalkerMessages.remove(ack.getId());

    } else if (message instanceof MemberInfo) { // remote non-metrics-collector windtalkers -> local metrics-collector
      if (null == metricsCollectorActors) {
        LOGGER.warn("Local metrics collectors for a Metrics-Collection app.");
        return;
      }
      MemberInfo memberInfo = (MemberInfo) message;
      for (ActorRef metricsCollectorActor : metricsCollectorActors) {
        metricsCollectorActor.tell(memberInfo, getSender());
      }

    } else if (message instanceof CurrentClusterState) {
      LOGGER.trace("Processing CurrentClusterState event.");
      CurrentClusterState state = (CurrentClusterState) message;
      for (Member member : state.getMembers()) {
        if (member.status().equals(MemberStatus.up())) {
          registerMember(member);
        }
      }

    } else if (message instanceof MemberUp) {
      LOGGER.trace("Processing MemberUp event.");
      MemberUp mUp = (MemberUp) message;
      LOGGER.trace("Member {} is up.", mUp.member().toString());
      registerMember(mUp.member());

    } else {
      unhandled(message);
      LOGGER.trace("Unhandled message: {}.", message);
    }
  }

  private void notifyPredecessorsIfExist() {
    Member member;
    while (null != (member = predecessorMembers.poll())) {
      LOGGER.info("Notified predecessor {}.", member);
      notifyMember(member, createRemoteActorsMessage());
    }
  }

  private void informLocalActors(RemoteMetricsCollectorActors remoteMetricsCollectorActors) {
    LOGGER.debug("Informing local actors with metrics-collector: {}.", remoteMetricsCollectorActors.getActors());
    if (isLocalActorsEmpty()) {
      LOGGER.error("No local user actors started.");
      return;
    }

    for (ActorRef localActor : localActors.getActors().keySet()) {
      localActor.tell(remoteMetricsCollectorActors, getSelf());
      LOGGER.debug("Informed: {} with metrics-collector.", localActor);
    }
  }

  private void informLocalActors(RemoteActors remoteActors) {
    LOGGER.debug("Informing local actors with: {}.", remoteActors.getActors());
    if (isLocalActorsEmpty()) {
      LOGGER.error("Skipped, no local user actors.");
      return;
    }

    for (Map.Entry<ActorRef, Role> localActor : localActors.getActors().entrySet()) {
      String role = localActor.getValue().getRole();
      for (Map.Entry<ActorRef, Role> remoteActor : remoteActors.getActors().entrySet()) {
        Set<String> remoteActorInterestedRoles = remoteActor.getValue().getInterestedRoles();
        if (remoteActorInterestedRoles.contains(role)) {
          localActor.getKey().tell(
              new SuccessorActor(remoteActor.getValue().getRole(), remoteActor.getKey()),
              getSelf()
          );
          LOGGER.debug("Informed: {} of {}.", localActor, remoteActor);
        }
      }
    }
  }

  private boolean isLocalActorsEmpty() {
    return null == localActors || null == localActors.getActors() || localActors.getActors().isEmpty();
  }

  private void notifyMetricsCollectorsWithMemberInfo(ActorRef destination) {
    Pipeline pipeline = Pipeline.getInstance();
    MemberInfo memberInfo = new MemberInfo(
        pipeline.getApplicationName(),
        Joiner.on(',').join(pipeline.getRoles()));

    destination.tell(memberInfo, getSelf());
  }

  private void registerMember(Member member) {
    LOGGER.debug("Registering member {}, with roles: {}.", member.address(), member.getRoles());

    if (isMemberCurrentRole(member)) {
      LOGGER.debug("Skipped, same role as current one.");
      return;
    }

    if (null != metricsCollectorActors) {
      LOGGER.debug("Notified {} with metrics-collector.", member);
      notifyMemberWithMetricsCollector(member);
    } else {
      allMembers.add(member);
    }

    if (null == interestedRoles) {
      LOGGER.debug("Skipped predecessor notifications for Hydrant.");
      return;
    }

    for (String role : interestedRoles) {
      if (member.hasRole(role)) {
        if (isLocalActorsEmpty()) {
          predecessorMembers.offer(member);
        } else {
          notifyMember(member, createRemoteActorsMessage());
          startResendingThread();
        }
      }
    }
  }

  private boolean isMemberCurrentRole(Member member) {
    Set<String> roles = Pipeline.getInstance().getRoles();
    for (String role : roles) {
      if (member.hasRole(role)) {
        return true;
      }
    }
    return false;
  }

  private RemoteActors createRemoteActorsMessage() {
    return new RemoteActors(localActors.getActors());
  }

  private void notifyMember(Member member, PipelineMessage message) {
    String predecessorWindtalkerAddress = member.address() + "/user/" + NAME;
    ActorSelection predecessor = getContext().actorSelection(predecessorWindtalkerAddress);

    predecessor.tell(message, getSelf());
    windtalkerMessages.put(message.getId(), new MessageCache<>(predecessor, message));

    LOGGER.debug("Notified windtalker {}.", predecessorWindtalkerAddress);
  }

  private void notifyAllMembers() {
    LOGGER.info("Notifying all members...");
    if (null == metricsCollectorActors || allMembers.isEmpty()) {
      LOGGER.info("Skipped, metricsCollectorActors is null or members list is empty.");
      return;
    }

    for (Member member : allMembers) {
      notifyMemberWithMetricsCollector(member);
    }
    startResendingThread();
    LOGGER.info("Notified all members, size: {}", allMembers.size());
  }

  private void notifyMemberWithMetricsCollector(Member member) {
    notifyMember(member, new RemoteMetricsCollectorActors(metricsCollectorActors));
  }

  private void startResendingThread() {
    LOGGER.debug("Started resending thread.");
    final int maxEmptyLoop = 3;
    final int maxLoop = 5;
    final MutableInt loopCount = new MutableInt(0);
    scheduled = scheduler.scheduleWithFixedDelay(() -> {
      int emptyLoop = 0;
      if (windtalkerMessages.isEmpty()) {
        if (++emptyLoop >= maxEmptyLoop) {
          stopResendingThread();
        }
        return;
      }
      LOGGER.debug("Resending un-acked {} message, remains {}.", windtalkerMessages.size(), maxLoop - loopCount.getValue());
      for (MessageCache<ActorSelection, PipelineMessage> messageCache : windtalkerMessages.values()) {
        messageCache.getDestination().tell(messageCache.getMessage(), getSelf());
      }
      if (loopCount.increase() >= maxLoop) {
        clearWindtalkerMessages();
        stopResendingThread();
      }
    }, 2, 2, TimeUnit.SECONDS);
  }

  private void clearWindtalkerMessages() {
    windtalkerMessages.clear();
  }

  private void stopResendingThread() {
    LOGGER.debug("Stopped resending thread.");
    scheduled.cancel(true);
    scheduled = null;
  }
}
