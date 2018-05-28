package org.gradoop.flink.model.impl.operators.subgraph;

import org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayoutFactory;

public class ApplySubgraphTxLayoutTest extends ApplySubgraphTest {

  public ApplySubgraphTxLayoutTest() {
    setCollectionLayoutFactory(new TxCollectionLayoutFactory());
  }
}
