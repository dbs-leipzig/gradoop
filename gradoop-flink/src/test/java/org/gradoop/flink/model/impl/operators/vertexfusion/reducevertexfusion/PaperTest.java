package org.gradoop.flink.model.impl.operators.vertexfusion.actualtests;

import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.operators.vertexfusion.AbstractPaperTest;
import org.gradoop.flink.model.impl.operators.fusion.ReduceVertexFusion;

public class PaperTest extends AbstractPaperTest {
  @Override
  protected GraphGraphCollectionToGraphOperator op() {
    return new ReduceVertexFusion();
  }
}
