package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.fusion.edgereduce.EdgeReduceVertexFusion;
import org.gradoop.flink.model.impl.operators.fusion.reduce.ReduceVertexFusion;

/**
 * Created by vasistas on 23/02/17.
 */
public class TestReduceWithEdges extends ReduceVertexFusionPaperTest {
  @Override
  protected void testGraphGraphGraphCollection(LogicalGraph left, LogicalGraph right,
    GraphCollection gcl, LogicalGraph expected) throws Exception {
    EdgeReduceVertexFusion f = new EdgeReduceVertexFusion();
    ReduceCombination rc = new ReduceCombination();
    LogicalGraph finitaryunion = rc.execute(gcl);
    LogicalGraph output = f.execute(left, right, finitaryunion);
    collectAndAssertTrue(output.equalsByData(expected));
  }
}
