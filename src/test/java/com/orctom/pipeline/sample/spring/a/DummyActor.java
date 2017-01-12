package com.orctom.pipeline.sample.spring.a;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.orctom.pipeline.ActorFactory;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.util.IdUtils;
import com.orctom.rmq.Message;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Actor
public class DummyActor extends UntypedActor {

  private ActorRef roleA = ActorFactory.actorOf(RoleA.class);

  private ActorRef roleA2 = ActorFactory.actorOf(RoleA2.class);

  @Override
  public void preStart() {
    System.out.println("started dummy ...........");
    ExecutorService es = Executors.newSingleThreadExecutor();
    es.submit(() -> {
      for (int i = 0; i < 30; i++) {
        Message msg = new Message(
            IdUtils.generate(),
            RandomStringUtils.randomAlphanumeric(400).getBytes()
        );
        if (i % 2 == 0) {
          roleA.tell(msg, getSelf());
        } else {
          roleA2.tell(msg, getSelf());
        }
        try {
          TimeUnit.MILLISECONDS.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    unhandled(message);
  }
}
