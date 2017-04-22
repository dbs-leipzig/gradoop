/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.fusion.vertex.functions;

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
 */
public class UpdateEdgesThoughToBeFusedVertices implements FlatJoinFunction<Edge, Vertex, Edge> {
  /**
   * Reusable edge that is used to update the previous edges in order to connect them with the
   * new fused vertex
   */
  private static final Edge REUSABLE_EDGE = new Edge();

  /**
   * the fused vertex's id
   */
  private GradoopId vId;

  /**
   * Checks if I have to check the fused vertex among the edges' sources or not.
   */
  private boolean isSourceNow;

  /**
   * Given the parameters, I have the edge update utility function
   * @param vId       Id for the fused vertex
   * @param isSource  If the fusedVertex is considered as source or as a destination
   */
  public UpdateEdgesThoughToBeFusedVertices(GradoopId vId, boolean isSource) {
    this.vId = vId;
    this.isSourceNow = isSource;
  }

  @Override
  public void join(Edge edge, Vertex vertex, Collector<Edge> collector) throws Exception {
    if (vertex == null) {
      collector.collect(edge);
    } else if (edge != null) {
      REUSABLE_EDGE.setId(GradoopId.get());
      REUSABLE_EDGE.setSourceId(isSourceNow ? vId : edge.getSourceId());
      REUSABLE_EDGE.setTargetId(isSourceNow ? edge.getTargetId() : vId);
      REUSABLE_EDGE.setProperties(edge.getProperties());
      REUSABLE_EDGE.setLabel(edge.getLabel());
      REUSABLE_EDGE.setGraphIds(edge.getGraphIds());
      collector.collect(REUSABLE_EDGE);
    }
  }
}
