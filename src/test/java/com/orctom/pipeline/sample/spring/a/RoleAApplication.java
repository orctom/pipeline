package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.Pipeline;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.Jedis;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.a",
    "com.orctom.pipeline.sample.spring.service"
})
public class RoleAApplication {

  @Bean
  public Jedis getJedis() {
    return new Jedis("localhost", 6379);
  }

  public static void main(String[] args) {
    Pipeline.getInstance()
        .withApplicationName("roleA")
        .run(RoleAApplication.class);
  }
}
