package org.gradoop.model.impl.functions.joinfunctions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.VertexData;

/**
 * Used when joining edges and vertices and the edges are of interest.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsFirst("*->*")
public class EdgeVertexJoinKeepEdge<
  VD extends VertexData,
  ED extends EdgeData>
  implements JoinFunction<ED, VD, ED> {

  /**
   * {@inheritDoc}
   */
  @Override
  public ED join(ED edge, VD vertex) throws Exception {
    return edge;
  }
}
