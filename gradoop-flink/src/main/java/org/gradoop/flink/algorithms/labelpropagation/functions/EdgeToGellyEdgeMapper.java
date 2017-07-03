
package org.gradoop.flink.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Maps EPGM edge to a Gelly edge consisting of EPGM source and target
 * identifier and {@link NullValue} as edge value.
 */
@FunctionAnnotation.ForwardedFields("sourceId->f0;targetId->f1")
public class EdgeToGellyEdgeMapper implements
  MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, NullValue>> {
  /**
   * Reduce object instantiations
   */
  private final org.apache.flink.graph.Edge<GradoopId, NullValue> reuseEdge;

  /**
   * Constructor
   */
  public EdgeToGellyEdgeMapper() {
    reuseEdge = new org.apache.flink.graph.Edge<>();
    reuseEdge.setValue(NullValue.getInstance());
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, NullValue> map(Edge epgmEdge)
      throws Exception {
    reuseEdge.setSource(epgmEdge.getSourceId());
    reuseEdge.setTarget(epgmEdge.getTargetId());
    return reuseEdge;
  }
}
