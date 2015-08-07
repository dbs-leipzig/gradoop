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
import org.apache.flink.api.common.functions.GroupReduceFunction;
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

import java.util.Iterator;

public class Difference<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> extends
  AbstractBinaryCollectionToCollectionOperator<VD, ED, GD> {
  @Override
  protected EPGraphCollection<VD, ED, GD> executeInternal(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondGraphCollection) throws Exception {
    DataSet<Tuple2<Subgraph<Long, GD>, Long>> thisGraphs =
      firstSubgraphs.map(new Tuple2LongMapper<Subgraph<Long, GD>>(1l));

    DataSet<Tuple2<Subgraph<Long, GD>, Long>> otherGraphs =
      secondSubgraphs.map(new Tuple2LongMapper<Subgraph<Long, GD>>(2l));

    final DataSet<Subgraph<Long, GD>> newSubgraphs =
      thisGraphs.union(otherGraphs)
        .groupBy(new SubgraphTupleKeySelector<GD, Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<Subgraph<Long, GD>, Long>,
          Subgraph<Long, GD>>() {

          @Override
          public void reduce(
            Iterable<Tuple2<Subgraph<Long, GD>, Long>> iterable,
            Collector<Subgraph<Long, GD>> collector) throws Exception {
            Iterator<Tuple2<Subgraph<Long, GD>, Long>> it = iterable.iterator();
            Tuple2<Subgraph<Long, GD>, Long> subgraph = null;
            Boolean inOtherCollection = false;
            while (it.hasNext()) {
              subgraph = it.next();
              if (subgraph.f1.equals(2l)) {
                inOtherCollection = true;
              }
            }
            if (!inOtherCollection && subgraph != null) {
              collector.collect(subgraph.f0);
            }
          }
        });

    DataSet<Vertex<Long, VD>> thisVertices =
      firstGraph.getVertices().union(secondGraph.getVertices())
        .distinct(new KeySelectors.VertexKeySelector<VD>());

    DataSet<Tuple2<Vertex<Long, VD>, Long>> verticesWithGraphs = thisVertices
      .flatMap(
        new FlatMapFunction<Vertex<Long, VD>, Tuple2<Vertex<Long, VD>, Long>>
          () {
          @Override
          public void flatMap(Vertex<Long, VD> VD,
            Collector<Tuple2<Vertex<Long, VD>, Long>> collector) throws
            Exception {
            for (Long graph : VD.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(VD, graph));
            }
          }
        });
    DataSet<Vertex<Long, VD>> vertices =
      verticesWithGraphs.join(newSubgraphs).where(1)
        .equalTo(new KeySelectors.GraphKeySelector<GD>()).with(
        new JoinFunction<Tuple2<Vertex<Long, VD>, Long>, Subgraph<Long, GD>,
          Vertex<Long, VD>>() {

          @Override
          public Vertex<Long, VD> join(
            Tuple2<Vertex<Long, VD>, Long> vertexLongTuple,
            Subgraph<Long, GD> subgraph) throws Exception {
            return vertexLongTuple.f0;
          }
        }).distinct(new KeySelectors.VertexKeySelector<VD>());

    DataSet<Edge<Long, ED>> edges = firstGraph.getEdges().join(vertices)
      .where(new KeySelectors.EdgeSourceVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>()).join(vertices)
      .where(new KeySelectors.EdgeTargetVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>())
      .distinct(new KeySelectors.EdgeKeySelector<ED>());

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
