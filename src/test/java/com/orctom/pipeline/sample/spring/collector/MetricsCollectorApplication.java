package com.orctom.pipeline.sample.spring.collector;

import com.orctom.pipeline.Pipeline;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
    "com.orctom.pipeline.sample.spring.collector",
    "com.orctom.pipeline.sample.spring.service"
})
public class MetricsCollectorApplication {

  public static void main(String[] args) {
    Pipeline.getInstance()
        .withCluster("dummy")
        .withName("collector")
        .run(MetricsCollectorApplication.class);
  }
}
