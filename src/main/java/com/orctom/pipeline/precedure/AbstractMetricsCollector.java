package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import com.orctom.laputa.model.Metric;
import com.orctom.pipeline.model.MemberInfo;
import com.orctom.pipeline.model.PipelineMetrics;

import java.util.HashMap;
import java.util.Map;

import static com.orctom.pipeline.Constants.MEMBER_EVENT_DOWN;
import static com.orctom.pipeline.Constants.MEMBER_EVENT_UP;

public abstract class AbstractMetricsCollector extends UntypedActor {

  protected Map<ActorRef, MemberInfo> members = new HashMap<>();

  @Override
  public final void onReceive(Object message) throws Throwable {
    if (message instanceof PipelineMetrics) {
      onMessage((PipelineMetrics) message);

    } else if (message instanceof MemberInfo) {
      MemberInfo memberInfo = (MemberInfo) message;
      ActorRef actor = getSender();
      members.put(actor, memberInfo);
      memberAdded(actor, memberInfo);
      onMessage(createPipelineMetrics(memberInfo, MEMBER_EVENT_UP));
      getContext().watch(getSender());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      ActorRef terminatedActor = terminated.getActor();
      MemberInfo memberInfo = members.remove(terminatedActor);
      memberRemoved(terminatedActor);
      if (null != memberInfo) {
        onMessage(createPipelineMetrics(memberInfo, MEMBER_EVENT_DOWN));
      }

    } else {
      unhandled(message);
    }
  }

  private PipelineMetrics createPipelineMetrics(MemberInfo memberInfo, String memberEvent) {
    return new PipelineMetrics(
        memberInfo.getApplicationName(),
        memberInfo.getRoles(),
        new Metric(memberEvent, "@" + memberInfo.getTimestamp())
    );
  }

  protected void memberAdded(ActorRef actor, MemberInfo memberInfo) {
  }

  protected void memberRemoved(ActorRef actorRef) {
  }

  protected abstract void onMessage(PipelineMetrics metric);
}
