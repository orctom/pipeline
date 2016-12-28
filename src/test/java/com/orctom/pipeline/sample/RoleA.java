package com.orctom.pipeline.sample;

import akka.actor.Props;
import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.Pipeline;
import com.orctom.pipeline.precedure.Hydrant;
import com.orctom.pipeline.util.SpringActorProducer;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class RoleA extends Hydrant {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  static final String ID = "roleA";

  @Override
  protected void run() {
    Props.create(SpringActorProducer.class);
    int noOfWorkers = 1;
    ExecutorService es = Executors.newFixedThreadPool(noOfWorkers);
    for (int i = 0; i < noOfWorkers; i++) {
      es.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            DummyMessage msg = new DummyMessage(RandomStringUtils.randomAlphanumeric(400));
            sendToSuccessors(msg);
            metrics.mark(ID);
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }
    es.shutdown();
  }

  public static void main(String[] args) {
    String cluster = "dummy";
    final Pipeline pipeline = Pipeline.create(cluster, RoleA.ID);
    pipeline.createActor(RoleA.ID, RoleA.class);
    pipeline.start();
  }
}
