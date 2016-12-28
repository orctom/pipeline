package com.orctom.pipeline.sample.spring.a;

import com.orctom.pipeline.PipelineApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.a",
    "com.orctom.pipeline.sample.spring.service"
})
public class RoleAApplication {

  public static void main(String[] args) {
    PipelineApplication.getInstance()
        .withCluster("dummy")
        .withRole("roleA")
        .run(RoleAApplication.class);
  }
}
