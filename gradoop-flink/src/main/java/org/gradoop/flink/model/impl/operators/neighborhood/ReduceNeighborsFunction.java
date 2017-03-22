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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborEdgeReduceFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.functions.NeighborVertexReduceFunction;
import org.gradoop.flink.model.impl.operators.neighborhood.keyselector.GradoopIdInTuple;

public class ReduceNeighborsFunction extends NeighborsFunction {

  public ReduceNeighborsFunction(VertexAggregateFunction function, EdgeDirection direction) {
    super(function, direction);
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<Vertex> vertices;
    switch (getDirection()) {
    case IN:
      vertices = graph.getEdges()
        .join(graph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .join(graph.getVertices())
        .where(new GradoopIdInTuple<Tuple2<Edge, Vertex>>(true, false, 0)).equalTo(new Id<>())
        .groupBy(new GradoopIdInTuple<Tuple2<Tuple2<Edge, Vertex>, Vertex>>(0, 1))
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));
      break;
    case OUT:
      vertices = graph.getEdges()
        .join(graph.getVertices())
        .where(new SourceId<>()).equalTo(new Id<>())
        .join(graph.getVertices())
        .where(new GradoopIdInTuple<Tuple2<Edge, Vertex>>(false, true, 0)).equalTo(new Id<>())
        .groupBy(new GradoopIdInTuple<Tuple2<Tuple2<Edge, Vertex>, Vertex>>(0, 1))
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));
//        graph.getEdges()
//        .join(graph.getVertices())
//        .where(new SourceId<>()).equalTo(new Id<>())
//        .groupBy(0)
//        .reduceGroup(new NeighborEdgeReduceFunction((EdgeAggregateFunction) getFunction()));
      break;
    case BOTH:
      vertices = graph.getEdges()
        .join(graph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .join(graph.getVertices())
        .where(new GradoopIdInTuple<Tuple2<Edge, Vertex>>(true, false, 0)).equalTo(new Id<>())
        .union(graph.getEdges()
          .join(graph.getVertices())
          .where(new SourceId<>()).equalTo(new Id<>())
          .join(graph.getVertices())
          .where(new GradoopIdInTuple<Tuple2<Edge, Vertex>>(false, true, 0)).equalTo(new Id<>()))
        .groupBy(new GradoopIdInTuple<Tuple2<Tuple2<Edge, Vertex>, Vertex>>(0, 1))
        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()));


//        .groupBy(new GradoopIdInTuple<Tuple2<Tuple2<Edge, Vertex>, Vertex>>(0, 1))
//        .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction()))
//        .union(graph.getEdges()
//          .join(graph.getVertices())
//          .where(new SourceId<>()).equalTo(new Id<>())
//          .join(graph.getVertices())
//          .where(new GradoopIdInTuple<Tuple2<Edge, Vertex>>(false, true, 0)).equalTo(new Id<>())
//          .groupBy(new GradoopIdInTuple<Tuple2<Tuple2<Edge, Vertex>, Vertex>>(0, 1))
//          .reduceGroup(new NeighborVertexReduceFunction((VertexAggregateFunction) getFunction())));
      break;
    default:
      vertices = null;
    }
    return LogicalGraph.fromDataSets(
      graph.getGraphHead(), vertices, graph.getEdges(), graph.getConfig());
  }

  @Override
  public String getName() {
    return ReduceNeighborsFunction.class.getName();
  }
}
