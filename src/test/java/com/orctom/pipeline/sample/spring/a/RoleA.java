package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.precedure.Pipe;
import com.orctom.pipeline.sample.spring.service.DummyService;
import com.orctom.rmq.Ack;
import com.orctom.rmq.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.concurrent.atomic.AtomicInteger;

@Actor("roleA")
public class RoleA extends Pipe {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoleA.class);

  @Resource
  private DummyService service;

  private AtomicInteger counter = new AtomicInteger(0);

  @Override
  public Ack onMessage(Message message) {
    sendToSuccessors(message);
    service.count("a1");
    LOGGER.debug("counter: {}", counter.incrementAndGet());
    return Ack.DONE;
  }
}
