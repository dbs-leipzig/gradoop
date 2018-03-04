package org.gradoop.flink.model.impl.operators.aggregation;

import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;

public class ApplyAggregationGVELayoutTest extends ApplyAggregationTest {

  public ApplyAggregationGVELayoutTest() {
    setCollectionLayoutFactory(new GVECollectionLayoutFactory());
  }
}
