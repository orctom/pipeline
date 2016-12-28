package com.orctom.pipeline.sample.spring.c;

import com.orctom.pipeline.PipelineApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.c",
    "com.orctom.pipeline.sample.spring.service"
})
public class RoleCApplication {

  public static void main(String[] args) {
    PipelineApplication.getInstance()
        .withCluster("dummy")
        .withRole("roleC")
        .withPredecessors("roleB")
        .run(RoleCApplication.class);
  }
}
