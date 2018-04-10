package org.gradoop.flink.model.impl.operators.selection;

import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;

public class SelectionGVELayoutTest extends SelectionTest {

  public SelectionGVELayoutTest() {
    setCollectionLayoutFactory(new GVECollectionLayoutFactory());
  }
}
