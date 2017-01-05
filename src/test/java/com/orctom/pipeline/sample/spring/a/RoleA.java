package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Hydrant;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.pipeline.util.IdUtils;
import com.orctom.rmq.Message;
import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Actor
public class RoleA extends Hydrant {

  @Resource
  private DummyService service;

  @Override
  protected void run() {
    System.out.println("starting dummy");
    System.out.println(service.foo());
    for (int i = 0; i < 1_000_000; i++) {
      Message msg = new Message(
          IdUtils.generate(),
          RandomStringUtils.randomAlphanumeric(400)
      );
      sendToSuccessors(msg);
    }
    try {
      TimeUnit.SECONDS.sleep(30);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
