package com.orctom.pipeline.sample;

import com.orctom.laputa.utils.SimpleMetrics;
import com.orctom.pipeline.precedure.Hydrant;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RoleA extends Hydrant {

  private SimpleMetrics metrics = SimpleMetrics.create(logger, 5, TimeUnit.SECONDS);

  private static final String KEY = "roleA";

  @Override
  protected void run() {
    int noOfWorkers = 1;
    ExecutorService es = Executors.newFixedThreadPool(noOfWorkers);
    for (int i = 0; i < noOfWorkers; i++) {
      es.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          DummyMessage msg = new DummyMessage(RandomStringUtils.randomAlphanumeric(400));
          sendToSuccessors(msg);
//            try {
//              TimeUnit.SECONDS.sleep(5);
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
          metrics.mark(KEY);
        }
      });
    }
    es.shutdown();
  }
}
