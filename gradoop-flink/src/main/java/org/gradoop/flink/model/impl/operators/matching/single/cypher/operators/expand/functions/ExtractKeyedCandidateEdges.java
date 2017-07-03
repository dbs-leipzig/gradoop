
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.tuples.EdgeWithTiePoint;

/**
 * Extract the join key from an edge embedding and stores both in an {@link EdgeWithTiePoint}
 */
public class ExtractKeyedCandidateEdges
  extends RichMapFunction<Embedding, EdgeWithTiePoint> {

  /**
   * Reuse Tuple
   */
  private EdgeWithTiePoint reuseEdgeWitTiePoint;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.reuseEdgeWitTiePoint = new EdgeWithTiePoint();
  }

  @Override
  public EdgeWithTiePoint map(Embedding edge) throws Exception {
    reuseEdgeWitTiePoint.setSource(edge.getId(0));
    reuseEdgeWitTiePoint.setId(edge.getId(1));
    if (edge.size() == 3) {
      // normal edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(2));
    } else {
      // loop edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(0));
    }

    return reuseEdgeWitTiePoint;
  }
}
