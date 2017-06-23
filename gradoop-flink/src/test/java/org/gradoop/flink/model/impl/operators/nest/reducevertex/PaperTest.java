package org.gradoop.flink.model.impl.operators.nest.reducevertex;

import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.nest.AbstractPaperTest;
import org.gradoop.flink.model.impl.operators.nest.ReduceVertexFusion;

public class PaperTest extends AbstractPaperTest {
  @Override
  protected GraphGraphCollectionToGraphOperator op() {
    return new ReduceVertexFusion();
  }
}
