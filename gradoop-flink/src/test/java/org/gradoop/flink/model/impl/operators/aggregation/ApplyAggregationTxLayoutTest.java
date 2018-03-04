package org.gradoop.flink.model.impl.operators.aggregation;

import org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayoutFactory;

public class ApplyAggregationTxLayoutTest extends ApplyAggregationTest {

  public ApplyAggregationTxLayoutTest() {
    setCollectionLayoutFactory(new TxCollectionLayoutFactory());
  }
}
