package com.orctom.pipeline.sample.spring.b;

import com.orctom.pipeline.Pipeline;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.b",
    "com.orctom.pipeline.sample.spring.service"
})
public class RoleBApplication {

  public static void main(String[] args) {
    Pipeline.getInstance()
        .withCluster("dummy")
        .withRole("roleB")
        .withPredecessors("roleA")
        .run(RoleBApplication.class);
  }
}
