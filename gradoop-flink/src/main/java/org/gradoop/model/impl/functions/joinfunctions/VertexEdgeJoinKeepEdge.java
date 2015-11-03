package org.gradoop.model.impl.functions.joinfunctions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;

/**
 * Used when joining vertices and edges and the edges are of interest.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 */
@FunctionAnnotation.ForwardedFieldsSecond("*->*")
public class VertexEdgeJoinKeepEdge<
  VD extends EPGMVertex,
  ED extends EPGMEdge>
  implements JoinFunction<VD, ED, ED> {

  /**
   * {@inheritDoc}
   */
  @Override
  public ED join(VD vertex, ED edge) throws Exception {
    return edge;
  }
}
