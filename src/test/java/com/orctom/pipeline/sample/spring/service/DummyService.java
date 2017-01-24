package com.orctom.pipeline.sample.spring.service;

import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;

@Service
public class DummyService {

  @Resource
  private Jedis jedis;

  public String foo() {
    return "bar";
  }

  public void count(String key) {
    jedis.incr(key);
  }
}
