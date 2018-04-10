package org.gradoop.flink.model.impl.operators.selection;

import org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayoutFactory;

public class SelectionTxLayoutTest extends SelectionTest {

  public SelectionTxLayoutTest() {
    setCollectionLayoutFactory(new TxCollectionLayoutFactory());
  }
}
