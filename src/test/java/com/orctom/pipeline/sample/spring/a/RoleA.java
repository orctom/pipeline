package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Hydrant;
import com.orctom.pipeline.sample.DummyMessage;
import com.orctom.pipeline.sample.spring.service.DummyService;
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
      DummyMessage msg = new DummyMessage(RandomStringUtils.randomAlphanumeric(400));
      sendToSuccessors(msg);
    }
  }
}
