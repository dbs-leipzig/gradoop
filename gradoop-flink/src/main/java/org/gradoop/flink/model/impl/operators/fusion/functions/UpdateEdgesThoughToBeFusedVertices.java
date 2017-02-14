package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * 1) If there is no vertex match (the vertex is null) then it means that the edge should not be
 *    updated
 * 2) If there is vertex match, it means that the matched vertex the fused one, so the edge
 *    should now point to the newly fused vertex
 * 3) Ignore the edges not matching with the vertices (pointless edges that should not occur and
 *    should be removed)
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
public class UpdateEdgesThoughToBeFusedVertices implements FlatJoinFunction<Edge, Vertex, Edge> {
  private GradoopId vId;
  private static final Edge reusableEdge = new Edge();

  public UpdateEdgesThoughToBeFusedVertices(GradoopId vId, boolean isSource) {
    this.vId = vId;
    this.isSourceNow = isSource;
  }

  private boolean isSourceNow;

  @Override
  public void join(Edge edge, Vertex vertex, Collector<Edge> collector) throws Exception {
    if (vertex == null) {
      collector.collect(edge);
    } else if (edge != null) {
      reusableEdge.setId(GradoopId.get());
      reusableEdge.setSourceId(isSourceNow ? vId : edge.getSourceId());
      reusableEdge.setTargetId(isSourceNow ? edge.getTargetId() : vId);
      reusableEdge.setProperties(edge.getProperties());
      reusableEdge.setLabel(edge.getLabel());
      reusableEdge.setGraphIds(edge.getGraphIds());
      collector.collect(reusableEdge);
    }
  }
}
