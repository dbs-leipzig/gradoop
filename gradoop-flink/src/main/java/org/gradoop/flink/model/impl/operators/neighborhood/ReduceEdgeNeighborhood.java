/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.neighborhood;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
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
        // group by the vertex id
        .groupBy(new IdInTuple<Tuple2<Edge, Vertex>>(1))
        .reduceGroup(new NeighborEdgeReduceFunction((EdgeAggregateFunction) getFunction()));
      break;
    default:
      vertices = null;
    }
    return graph.getConfig().getLogicalGraphFactory().fromDataSets(graph.getGraphHead(),
      vertices, graph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return ReduceEdgeNeighborhood.class.getName();
  }
}
