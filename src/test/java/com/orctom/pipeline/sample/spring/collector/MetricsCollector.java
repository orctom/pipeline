package com.orctom.pipeline.sample.spring.collector;

import com.orctom.pipeline.annotation.Actor;
import com.orctom.pipeline.model.PipelineMetrics;
import com.orctom.pipeline.precedure.AbstractMetricsCollector;
import com.orctom.pipeline.sample.spring.service.DummyService;

import javax.annotation.Resource;

@Actor("metrics-collector")
class MetricsCollector extends AbstractMetricsCollector {

  @Resource
  private DummyService service;

  @Override
  public void onMessage(PipelineMetrics metric) {
    service.foo();
    System.out.println(metric);
  }
}
