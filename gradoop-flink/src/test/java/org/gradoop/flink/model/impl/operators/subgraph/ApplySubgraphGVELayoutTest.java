package org.gradoop.flink.model.impl.operators.subgraph;

import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;

public class ApplySubgraphGVELayoutTest extends ApplySubgraphTest {

  public ApplySubgraphGVELayoutTest() {
    setCollectionLayoutFactory(new GVECollectionLayoutFactory());
  }
}
