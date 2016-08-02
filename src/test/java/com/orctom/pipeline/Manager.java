package com.orctom.pipeline;

import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Manager extends UntypedActor {

  private Cluster cluster = Cluster.get(getContext().system());

  @Override
  public void preStart() {
    cluster.subscribe(getSelf(),
        ClusterEvent.MemberUp.class,
        ClusterEvent.MemberExited.class,
        ClusterEvent.MemberRemoved.class,
        ClusterEvent.UnreachableMember.class,
        ClusterEvent.ReachableMember.class
    );
  }

  @Override
  public void postStop() {
    cluster.unsubscribe(getSelf());
  }

  @Override
  public void onReceive(Object message) {
    if (message instanceof ClusterEvent.CurrentClusterState) {
      ClusterEvent.CurrentClusterState state = (ClusterEvent.CurrentClusterState) message;
      for (Member member : state.getMembers()) {
        if (member.status().equals(MemberStatus.up())) {
          register(member);
        }
      }
    } else if (message instanceof ClusterEvent.MemberUp) {
      ClusterEvent.MemberUp event = (ClusterEvent.MemberUp) message;
      register(event.member());
    } else if (message instanceof ClusterEvent.MemberExited) {
      ClusterEvent.MemberExited event = (ClusterEvent.MemberExited) message;
      System.out.println("MemberExited: " + event.member());
    } else if (message instanceof ClusterEvent.MemberRemoved) {
      ClusterEvent.MemberRemoved event = (ClusterEvent.MemberRemoved) message;
      System.out.println("MemberRemoved: " + event.member());
    } else if (message instanceof ClusterEvent.UnreachableMember) {
      ClusterEvent.UnreachableMember event = (ClusterEvent.UnreachableMember) message;
      System.out.println("UnreachableMember: " + event.member());
    } else if (message instanceof ClusterEvent.ReachableMember) {
      ClusterEvent.ReachableMember event = (ClusterEvent.ReachableMember) message;
      System.out.println("ReachableMember: " + event.member());
    } else {
      unhandled(message);
    }
  }

  private void register(Member member) {
    System.out.println("roles  :" + member.roles());
    System.out.println("address: " + member.address());
    getContext().actorSelection(member.address() + "/user/frontend").tell("/cluster", getSelf());
  }
}
