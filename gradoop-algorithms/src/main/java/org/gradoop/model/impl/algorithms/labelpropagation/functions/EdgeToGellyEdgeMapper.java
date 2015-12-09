package org.gradoop.model.impl.algorithms.labelpropagation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Maps EPGM edge to a Gelly edge consisting of EPGM source and target
 * identifier and {@link NullValue} as edge value.
 *
 * @param <E> EPGM edge type
 */
public class EdgeToGellyEdgeMapper<E extends EPGMEdge>
  implements MapFunction<E, Edge<GradoopId, NullValue>> {
  /**
   * Reduce object instantiations
   */
  private final Edge<GradoopId, NullValue> reuseEdge;

  public EdgeToGellyEdgeMapper() {
    reuseEdge = new Edge<>();
  }

  @Override
  public Edge<GradoopId, NullValue> map(E epgmEdge)
    throws Exception {
    reuseEdge.setSource(epgmEdge.getSourceId());
    reuseEdge.setTarget(epgmEdge.getTargetId());
    reuseEdge.setValue(NullValue.getInstance());
    return reuseEdge;
  }
}
