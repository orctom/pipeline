package com.orctom.pipeline.sample.spring.b;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.PipeActor;
import com.orctom.pipeline.sample.spring.service.DummyService;

import javax.annotation.Resource;

@Actor
class RoleB extends PipeActor {

  @Resource
  private DummyService service;

  @Override
  protected void onMessage(Message message) {
    System.out.println(service.foo());
    System.out.println(message);
    sendToSuccessors(message);
  }
}
