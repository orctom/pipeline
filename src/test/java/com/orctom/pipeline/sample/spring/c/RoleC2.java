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

@Actor(role = "roleC2", interestedRoles = "roleB2")
class RoleC2 extends Outlet {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoleC2.class);

  @Resource
  private DummyService service;

  private AtomicInteger counter = new AtomicInteger(0);

  @Override
  protected Ack onMessage(Message message) {
    service.count("c2");
    LOGGER.debug("counter: {}", counter.incrementAndGet());
    return Ack.DONE;
  }
}
