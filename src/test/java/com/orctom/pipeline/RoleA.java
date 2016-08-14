package com.orctom.pipeline;

import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.AbstractProcedure;
import com.orctom.pipeline.precedure.Hydrant;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RoleA extends Hydrant {

  @Override
  protected String getSuccessorRoleName() {
    return "roleB";
  }

  @Override
  protected void run() {
    int noOfWorkers = 5;
    ExecutorService es = Executors.newFixedThreadPool(noOfWorkers);
    for (int i = 0; i < noOfWorkers; i++) {
      es.submit(new Runnable() {
        @Override
        public void run() {
          while(!Thread.currentThread().isInterrupted()) {
            DummyMessage msg = new DummyMessage(RandomStringUtils.randomAlphanumeric(400));
            sendToSuccessor(msg);
          }
        }
      });
    }
    es.shutdown();
  }
}
