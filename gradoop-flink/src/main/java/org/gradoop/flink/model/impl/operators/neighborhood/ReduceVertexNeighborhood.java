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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborVertexReduceFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.ShuffledVertexIdsFromEdge;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexIdsFromEdge;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexToFieldOne;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexToFieldZero;
import org.gradoop.flink.model.impl.operators.neighborhood.keyselector.IdInTuple;

/**
 * Reduce vertex neighborhood operator.
 */
public class ReduceVertexNeighborhood extends VertexNeighborhood {

  /**
   * Valued constructor.
   *
   * @param function  vertex aggregate function
   * @param direction considered edge direction
   */
  public ReduceVertexNeighborhood(VertexAggregateFunction function, EdgeDirection direction) {
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
      // takes edges and gets the corresponding vertices and applies the aggregate function for
      // vertices of incoming edges
      vertices = graph.getEdges()
        // tuple of source id and target id
        .map(new VertexIdsFromEdge())
        .join(graph.getVertices())
        .where(1).equalTo(new Id<>())
        // replace the second id with the vertex
        .with(new VertexToFieldOne<GradoopId, GradoopId>())
        .join(graph.getVertices())
        // replace the first id with the vertex
        .where(0).equalTo(new Id<>())
        .with(new VertexToFieldZero<GradoopId, Vertex>())
        // group by the target vertex
        .groupBy(new IdInTuple<Tuple2<Vertex, Vertex>>(1))
        // aggregate values
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));
      break;
    case OUT:
      // takes edges and gets the corresponding vertices and applies the aggregate function for
      // vertices of outgoing edges
      vertices = graph.getEdges()
        // tuple of target id and source id
        .map(new VertexIdsFromEdge(true))
        .join(graph.getVertices())
        .where(1).equalTo(new Id<>())
        // replace the second id with the vertex
        .with(new VertexToFieldOne<GradoopId, GradoopId>())
        .join(graph.getVertices())
        // replace the first id with the vertex
        .where(0).equalTo(new Id<>())
        .with(new VertexToFieldZero<GradoopId, Vertex>())
        // group by the target vertex
        .groupBy(new IdInTuple<Tuple2<Vertex, Vertex>>(1))
        // aggregate values
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));
      break;
    case BOTH:
      // takes edges and gets the corresponding vertices and applies the aggregate function for
      // vertices of incoming and outgoing edges
      vertices = graph.getEdges()
        // maps source-target and target-source ids from the edge
        .flatMap(new ShuffledVertexIdsFromEdge())
        .join(graph.getVertices())
        .where(1).equalTo(new Id<>())
        // replace the second id with the vertex
        .with(new VertexToFieldOne<GradoopId, GradoopId>())
        .join(graph.getVertices())
        .where(0).equalTo(new Id<>())
        // replace the first id with the vertex
        .with(new VertexToFieldZero<GradoopId, Vertex>())
        .groupBy(new IdInTuple<Tuple2<Vertex, Vertex>>(1))
        // aggregate values
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));
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
    return ReduceVertexNeighborhood.class.getName();
  }
}
