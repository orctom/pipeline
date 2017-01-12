package com.orctom.pipeline.sample.spring.b;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Pipe;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

import javax.annotation.Resource;

@Actor(role="roleB2", interestedRoles = "roleA2")
class RoleB2 extends Pipe {

  @Resource
  private DummyService service;

  @Override
  protected Ack onMessage(Message message) {
//    System.out.println(service.foo());
//    System.out.println(message);
    sendToSuccessors(message);
    return Ack.DONE;
  }
}
