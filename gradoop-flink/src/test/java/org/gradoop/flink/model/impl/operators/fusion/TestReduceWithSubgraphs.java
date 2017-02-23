package org.gradoop.flink.model.impl.operators.fusion;

import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.reduce.ReduceVertexFusion;

/**
 * Created by vasistas on 23/02/17.
 */
public class TestReduceWithSubgraphs extends ReduceVertexFusionPaperTest {
  protected void testGraphGraphGraphCollection(LogicalGraph left, LogicalGraph right,
    GraphCollection gcl, LogicalGraph expected) throws Exception {
    ReduceVertexFusion f = new ReduceVertexFusion();
    LogicalGraph output = f.execute(left, right, gcl);
    collectAndAssertTrue(output.equalsByData(expected));
  }
}
