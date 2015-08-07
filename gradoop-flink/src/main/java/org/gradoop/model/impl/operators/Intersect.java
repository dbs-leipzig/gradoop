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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;

public class Intersect<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> extends
  AbstractBinaryCollectionToCollectionOperator<VD, ED, GD> {
  @Override
  protected EPGraphCollection<VD, ED, GD> executeInternal(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondGraphCollection) {
    final DataSet<Subgraph<Long, GD>> newSubgraphs =
      firstSubgraphs.union(secondSubgraphs)
        .groupBy(new KeySelectors.GraphKeySelector<GD>())
        .reduceGroup(new SubgraphGroupReducer<GD>(2));

    DataSet<Vertex<Long, VD>> thisVertices = firstGraph.getVertices();

    DataSet<Tuple2<Vertex<Long, VD>, Long>> verticesWithGraphs = thisVertices
      .flatMap(
        new FlatMapFunction<Vertex<Long, VD>, Tuple2<Vertex<Long, VD>, Long>>
          () {

          @Override
          public void flatMap(Vertex<Long, VD> v,
            Collector<Tuple2<Vertex<Long, VD>, Long>> collector) throws
            Exception {
            for (Long graph : v.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(v, graph));
            }
          }
        });

    DataSet<Vertex<Long, VD>> vertices =
      verticesWithGraphs.join(newSubgraphs).where(1)
        .equalTo(new KeySelectors.GraphKeySelector<GD>()).with(
        new JoinFunction<Tuple2<Vertex<Long, VD>, Long>, Subgraph<Long, GD>,
          Vertex<Long, VD>>() {

          @Override
          public Vertex<Long, VD> join(Tuple2<Vertex<Long, VD>, Long> vertices,
            Subgraph<Long, GD> subgraph) throws Exception {
            return vertices.f0;
          }
        });

    DataSet<Edge<Long, ED>> edges = firstGraph.getEdges().join(vertices)
      .where(new KeySelectors.EdgeSourceVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>()).join(vertices)
      .where(new KeySelectors.EdgeTargetVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>());

    return new EPGraphCollection<>(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, firstCollection.getVertexDataFactory(),
      firstCollection.getEdgeDataFactory(),
      firstCollection.getGraphDataFactory(), env);
  }

  @Override
  public String getName() {
    return "Difference";
  }
}
