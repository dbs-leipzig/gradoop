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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.tuple.SwitchPair;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborEdgeReduceFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexIdsWithEdge;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.VertexToFieldOne;
import org.gradoop.flink.model.impl.operators.neighborhood.keyselector.IdInTuple;

/**
 * Reduce edge neighborhood operator.
 */
public class ReduceEdgeNeighborhood extends EdgeNeighborhood {


  /**
   * Valued constructor.
   *
   * @param function  edge aggregate function
   * @param direction considered edge direction
   */
  public ReduceEdgeNeighborhood(EdgeAggregateFunction function, EdgeDirection direction) {
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
      vertices = graph.getEdges()
        .join(graph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .groupBy(new IdInTuple<Tuple2<Edge, Vertex>>(1))
        .reduceGroup(new NeighborEdgeReduceFunction((EdgeAggregateFunction) getFunction()));
      break;
    case OUT:
      // takes edges which start at the vertex and applies the aggregate function
      vertices = graph.getEdges()
        .join(graph.getVertices())
        .where(new SourceId<>()).equalTo(new Id<>())
        .groupBy(new IdInTuple<Tuple2<Edge, Vertex>>(1))
        .reduceGroup(new NeighborEdgeReduceFunction((EdgeAggregateFunction) getFunction()));
      break;
    case BOTH:
      // takes edges which start at and target to the vertex and applies the aggregate function
      vertices = graph.getEdges()
        // maps the source id to the edge and the target id to the edge
        .flatMap(new VertexIdsWithEdge())
        .map(new SwitchPair<GradoopId, Edge>())
        .join(graph.getVertices())
        .where(1).equalTo(new Id<>())
        // replace id with the vertex
        .with(new VertexToFieldOne<Edge, GradoopId>())
//        .where(new TargetId<>()).equalTo(new Id<>())
//        .union(graph.getEdges()
//          .join(graph.getVertices())
//          .where(new SourceId<>()).equalTo(new Id<>()))
        // group by the vertex id
        .groupBy(new IdInTuple<Tuple2<Edge, Vertex>>(1))
        .reduceGroup(new NeighborEdgeReduceFunction((EdgeAggregateFunction) getFunction()));
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
    return ReduceEdgeNeighborhood.class.getName();
  }
}
