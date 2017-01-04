package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Hydrant;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.rmq.Message;
import org.apache.commons.lang3.RandomStringUtils;

import javax.annotation.Resource;

@Actor
public class RoleA extends Hydrant {

  @Resource
  private DummyService service;

  @Override
  protected void run() {
    System.out.println("starting dummy");
    System.out.println(service.foo());
    for (int i = 0; i < 5; i++) {
      Message msg = new Message(
          RandomStringUtils.randomAlphanumeric(20),
          RandomStringUtils.randomAlphanumeric(400)
      );
      sendToSuccessors(msg);
    }
  }
}
