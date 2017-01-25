package com.orctom.pipeline.sample.spring.c;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Outlet;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;

@Actor(role = "roleC", interestedRoles = "roleB")
class RoleC extends Outlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoleC.class);

  @Resource
  private DummyService service;

  private AtomicInteger counter = new AtomicInteger(0);

  @Override
  public Ack onMessage(Message message) {
    service.count("c1");
    LOGGER.debug("counter: {}", counter.incrementAndGet());
    return Ack.DONE;
  }
}
