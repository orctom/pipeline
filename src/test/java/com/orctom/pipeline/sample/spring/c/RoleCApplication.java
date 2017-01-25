package com.orctom.pipeline.sample.spring.c;

import com.orctom.pipeline.Pipeline;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.c",
    "com.orctom.pipeline.sample.spring.service"
})
public class RoleCApplication {

  public static void main(String[] args) {
    Pipeline.getInstance()
        .withApplicationName("roleC")
        .run(RoleCApplication.class);
  }
}
