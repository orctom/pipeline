package com.orctom.pipeline.sample.spring.a;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.orctom.pipeline.ActorFactory;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.pipeline.util.IdUtils;
import com.orctom.rmq.Message;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Actor("dummy")
public class DummyActor extends UntypedActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyActor.class);

  private ActorRef roleA = ActorFactory.actorOf(RoleA.class);

  private ActorRef roleA2 = ActorFactory.actorOf(RoleA2.class);

  @Resource
  private DummyService service;

  private AtomicInteger counter1 = new AtomicInteger(0);
  private AtomicInteger counter2 = new AtomicInteger(0);

  @Override
  public void preStart() {
    System.out.println("started dummy ...........");
    ExecutorService es = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("test-dummy-%d").build()
    );
    long size = Pipeline.getInstance().getConfig().getLong("hydrant.size");
    es.submit(() -> {
      for (int i = 0; i < size; i++) {
        Message msg = new Message(
            IdUtils.generate(),
            RandomStringUtils.randomAlphanumeric(400).getBytes()
        );
        if (i % 2 == 0) {
          roleA.tell(msg, getSelf());
          service.count("d1");
          LOGGER.debug("counter1: {}", counter1.incrementAndGet());
        } else {
          roleA2.tell(msg, getSelf());
          service.count("d2");
          LOGGER.debug("counter2: {}", counter2.incrementAndGet());
        }
//        try {
//          TimeUnit.MILLISECONDS.sleep(1);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
      }
    });
  }

  @Override
  public void onReceive(Object message) throws Throwable {
    unhandled(message);
  }
}
