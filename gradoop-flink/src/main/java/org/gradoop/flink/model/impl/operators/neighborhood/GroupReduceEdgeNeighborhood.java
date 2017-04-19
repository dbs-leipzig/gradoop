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

package org.gradoop.flink.model.impl.operators.neighborhood;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborEdgeCoGroupFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborEdgesCoGroupFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexIdsWithEdge;

/**
 * Group reduce edge neighborhood operator.
 */
public class GroupReduceEdgeNeighborhood extends EdgeNeighborhood {

  /**
   * Valued constructor.
   *
   * @param function  edge aggregate function
   * @param direction considered edge direction
   */
  public GroupReduceEdgeNeighborhood(EdgeAggregateFunction function, EdgeDirection direction) {
    super(function, direction);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Vertex> vertices;
    switch (getDirection()) {
    case IN:
      // takes edges which target to the vertex and applies the aggregate function
      vertices = graph.getVertices()
        .coGroup(graph.getEdges())
        .where(new Id<>()).equalTo(new TargetId<>())
        .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction()));
      break;
    case OUT:
      // takes edges which start at the vertex and applies the aggregate function
      vertices = graph.getVertices()
        .coGroup(graph.getEdges())
        .where(new Id<>()).equalTo(new SourceId<>())
        .with(new NeighborEdgeCoGroupFunction((EdgeAggregateFunction) getFunction()));
      break;
    case BOTH:
      // takes edges which start at and target to the vertex and applies the aggregate function
      vertices = graph.getVertices()
        .coGroup(graph.getEdges()
          // maps source id to edge and target id to edge
          .flatMap(new VertexIdsWithEdge()))
        .where(new Id<>()).equalTo(0)
        .with(new NeighborEdgesCoGroupFunction((EdgeAggregateFunction) getFunction()));
      break;
    default:
      vertices = null;
    }
    return LogicalGraph.fromDataSets(
      graph.getGraphHead(), vertices, graph.getEdges(), graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return GroupReduceEdgeNeighborhood.class.getName();
  }
}
