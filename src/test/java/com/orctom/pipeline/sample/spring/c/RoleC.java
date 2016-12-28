package com.orctom.pipeline.sample.spring.c;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.Message;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.sample.spring.service.DummyService;

import javax.annotation.Resource;

@Actor
class RoleC extends Outlet {

  @Resource
  private DummyService service;

  @Override
  protected void onMessage(Message message) {
    System.out.println(service.foo());
    System.out.println(message);
  }
}
