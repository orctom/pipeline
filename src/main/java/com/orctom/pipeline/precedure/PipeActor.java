package com.orctom.pipeline.precedure;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.model.MessageAck;
import com.orctom.pipeline.model.RemoteMetricsCollectorActors;
import com.orctom.pipeline.model.SuccessorActor;
import com.orctom.pipeline.persist.MessageQueue;
import com.orctom.pipeline.util.RoleUtils;
import com.orctom.pipeline.util.SimpleMetricCallback;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import com.orctom.rmq.RMQConsumer;
import com.orctom.rmq.RMQOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.orctom.pipeline.Constants.*;

/**
 * Automatically notifyPredecessors / unregister predecessors and successors in the cluster,
 * So that current actor can get a list of live predecessors and successors.
 * Created by hao on 7/18/16.
 */
public abstract class PipeActor extends UntypedActor implements RMQConsumer {

  protected Logger logger = LoggerFactory.getLogger(getClass());

  SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);
  MessageQueue messageQueue = MessageQueue.getInstance(new RMQOptions(getRole()));

  private SimpleMetricCallback callback;

  private Successors successors = new Successors(getContext(), getSelf(), messageQueue, metrics, getRole());

  @Override
  public final void preStart() throws Exception {
    logger.debug("Staring actor: {}...", getSelf().toString());
    subscribeInbox();
    initMetricsCollectorCallback();
    started();
  }

  private void initMetricsCollectorCallback() {
    callback = new SimpleMetricCallback(getRole());
    metrics.setCallback(callback);
  }

  protected void subscribeInbox() {
    messageQueue.subscribe(Q_INBOX, this);
  }

  /**
   * Don't halt on this thread
   */
  protected void started() {
  }

  protected void sendToSuccessors(Message message) {
    logger.trace("sending to successor {}", message);
    messageQueue.push(Q_PROCESSED, message);
    metrics.mark(METER_PROCESSED);
  }

  @Override
  public final void onReceive(Object message) throws Exception {
    if (message instanceof Message) { // from predecessor pipe actors
      Message msg = (Message) message;
      MessageAck ack = new MessageAck(msg.getId());
      messageQueue.push(Q_INBOX, withRoleRemovedFromId(msg));
      getSender().tell(ack, getSelf());
      metrics.mark(METER_INBOX);

    } else if (message instanceof MessageAck) { // from successor pipe actors
      MessageAck msg = (MessageAck) message;
      messageQueue.delete(Q_SENT_RECORDS, msg.getId());

    } else if (message instanceof SuccessorActor) { // from windtalker
      SuccessorActor successorActor = (SuccessorActor) message;
      logger.debug("Linked with successor {}: {}", successorActor.getRole(), successorActor.getActor());
      addSuccessor(successorActor.getRole(), successorActor.getActor());

    } else if (message instanceof RemoteMetricsCollectorActors) { // from windtalker
      logger.info("Received metrics-collector");
      RemoteMetricsCollectorActors msg = (RemoteMetricsCollectorActors) message;
      for (ActorRef actorRef : msg.getActors()) {
        getContext().watch(actorRef);
      }
      callback.addCollectors(msg.getActors());

    } else if (message instanceof Terminated) {
      Terminated terminated = (Terminated) message;
      successors.remove(terminated.getActor());
      callback.removeCollector(terminated.getActor());
      logger.warn("Routee {} terminated.", terminated.getActor().toString());

    } else {
      unhandled(message);
      logger.warn("Unhandled message: {}.", message);
    }
  }

  /**
   * resent messages contains role name in id, as <code>14545666@roleB</code>
   */
  private Message withRoleRemovedFromId(Message msg) {
    String id = msg.getId();
    int atSignIndex = id.indexOf(AT_SIGN);
    if (atSignIndex < 0) {
      return msg;
    }

    return new Message(id.substring(0, atSignIndex), msg.getData());
  }

  public abstract Ack onMessage(Message message);

  private void addSuccessors(String role, List<ActorRef> actorRefs) {
    for (ActorRef actorRef : actorRefs) {
      addSuccessor(role, actorRef);
    }
  }

  private void addSuccessor(String role, ActorRef actorRef) {
    logger.debug("Adding as routee {}.", actorRef.toString());
    if (successors.addSuccessor(role, actorRef)) {
      getContext().watch(actorRef);
      logSuccessors();

    } else {
      logger.debug("Already exists.");
    }
  }

  private void logSuccessors() {
    metrics.setGaugeIfNotExist("routee", () -> successors.getRoles());
  }

  public String getRole() {
    return RoleUtils.getRole(getClass()).getRole();
  }

  @Override
  public final String toString() {
    return getSelf().toString();
  }
}
