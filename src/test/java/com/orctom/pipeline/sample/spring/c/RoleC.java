package com.orctom.pipeline.sample.spring.c;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.PipelineMessage;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;

import javax.annotation.Resource;

@Actor
class RoleC extends Outlet {

  @Resource
  private DummyService service;

  @Override
  protected Ack onMessage(Message message) {
    System.out.println(service.foo());
    System.out.println(message);
    return Ack.DONE;
  }
}
