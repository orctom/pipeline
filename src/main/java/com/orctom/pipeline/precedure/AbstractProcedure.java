package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Router;
import com.google.common.base.Strings;
import com.orctom.pipeline.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static akka.cluster.ClusterEvent.CurrentClusterState;
import static akka.cluster.ClusterEvent.MemberUp;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class AbstractProcedure extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcedure.class);

  public static final String PREDECESSOR_UP = "_PREDECESSOR_UP_";
  public static final String PREDECESSOR_UP_ACK = "_PREDECESSOR_UP_ACK_";
  public static final String SUCCESSOR_UP = "_SUCCESSOR_UP_";

  private Cluster cluster = Cluster.get(getContext().system());

  private Set<Member> predecessors = new HashSet<>();
  private Router successors = new Router(new RoundRobinRoutingLogic());

  protected abstract String getPredecessorRoleName();

  protected abstract String getSuccessorRoleName();

  protected abstract void onMessage(Message message);

  @Override
  public final void preStart() throws Exception {
    validate();
    cluster.subscribe(getSelf(), MemberUp.class);
    beforeStart();
  }

  protected void beforeStart() {
  }

  @Override
  public final void postStop() throws Exception {
    cluster.unsubscribe(getSelf());
    afterStop();
  }

  protected void afterStop() {
  }

  protected void validate() {
    if (Strings.isNullOrEmpty(getPredecessorRoleName())) {
      throw new IllegalArgumentException("getPredecessorRoleName() not providing validate value");
    }

    if (Strings.isNullOrEmpty(getSuccessorRoleName())) {
      throw new IllegalArgumentException("getSuccessorRoleName() not providing validate value");
    }
  }

  protected boolean isSuccessorsAvailable() {
    return !successors.routees().isEmpty();
  }

  protected void sendToSuccessor(Message message) {
    if (!isSuccessorsAvailable()) {
      LOGGER.error("No available successors, discarding...");
      return;
    }
    successors.route(message, getSelf());
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof Message) {
      onMessage((Message) message);

    } else if (message instanceof CurrentClusterState) {
      CurrentClusterState state = (CurrentClusterState) message;
      for (Member member : state.getMembers()) {
        if (member.status().equals(MemberStatus.up())) {
          registerMember(member);
        }
      }

    } else if (message instanceof MemberUp) {
      MemberUp mUp = (MemberUp) message;
      registerMember(mUp.member());

    } else if (message.equals(PREDECESSOR_UP)) {
      getSender().tell(PREDECESSOR_UP_ACK, getSelf());

    } else if (message.equals(PREDECESSOR_UP_ACK)) {
      addSuccessorToRoutee(getSender());

    } else if (message.equals(SUCCESSOR_UP)) {
      addSuccessorToRoutee(getSender());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors.removeRoutee(terminated.getActor());

    } else {
      unhandled(message);
    }
  }

  private void addSuccessorToRoutee(ActorRef routee) {
    if (successors.routees().contains(routee)) {
      return;
    }

    getContext().watch(routee);
    successors.addRoutee(routee);
  }

  private void registerMember(Member member) {
    if (member.hasRole(getPredecessorRoleName()) && !predecessors.contains(member)) {
      notifyPredecessors(member);
    } else if (member.hasRole(getSuccessorRoleName()) && !successors.routees().contains(member)) {
      notifySuccessor(member);
    }
  }

  private void notifyPredecessors(Member member) {
    getContext().actorSelection(member.address().toString()).tell(SUCCESSOR_UP, getSelf());
    predecessors.add(member);
  }

  private void notifySuccessor(Member member) {
    getContext().actorSelection(member.address().toString()).tell(PREDECESSOR_UP, getSelf());
  }
}
