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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborVerticesCoGroupFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborVertexCoGroupFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.ShuffledVertexIdsFromEdge;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexIdsFromEdge;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexToFieldOne;

/**
 * Group reduce vertex neighborhood operator.
 */
public class GroupReduceVertexNeighborhood extends VertexNeighborhood {

  /**
   * Valued constructor.
   *
   * @param function  vertex aggregate function
   * @param direction considered edge direction
   */
  public GroupReduceVertexNeighborhood(VertexAggregateFunction function, EdgeDirection direction) {
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
      // takes vertices from edges which target to the vertex and applies the aggregate function
      vertices = graph.getVertices()
        .coGroup(
          graph.getEdges()
            .map(new VertexIdsFromEdge(true))
          .join(graph.getVertices())
          // map the source vertex of an edge
          .where(1).equalTo(new Id<>())
          .with(new VertexToFieldOne<GradoopId, GradoopId>()))
        // take all source vertices of an edge which target the current vertex
        .where(new Id<>()).equalTo(0)
        // aggregate their values
        .with(new NeighborVertexCoGroupFunction((VertexAggregateFunction) getFunction()));
      break;
    case OUT:
      // takes vertices from edges which start at the vertex and applies the aggregate function
      vertices = graph.getVertices()
        .coGroup(
          graph.getEdges()
            .map(new VertexIdsFromEdge())
            .join(graph.getVertices())
            // map the target vertex of an edge
            .where(1).equalTo(new Id<>())
            .with(new VertexToFieldOne<GradoopId, GradoopId>()))
        // take all source vertices of an edge which target the current vertex
        .where(new Id<>()).equalTo(0)
        // aggregate their values
        .with(new NeighborVertexCoGroupFunction((VertexAggregateFunction) getFunction()));
      break;
    case BOTH:
      // takes vertices from edges which start at and target to the vertex and applies the aggregate
      // function
      vertices = graph.getVertices()
        .coGroup(graph.getEdges()
          // maps source-target and target-source ids from the edge
          .flatMap(new ShuffledVertexIdsFromEdge())
          .join(graph.getVertices())
          // map one vertex
          .where(1).equalTo(new Id<>())
          .with(new VertexToFieldOne<GradoopId, GradoopId>()))
        // take all opposite vertices of an edge of which the current vertex is part of
        .where(new Id<>()).equalTo(0)
        // aggregate their values
        .with(new NeighborVerticesCoGroupFunction((VertexAggregateFunction) getFunction()));
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
    return GroupReduceVertexNeighborhood.class.getName();
  }
}
