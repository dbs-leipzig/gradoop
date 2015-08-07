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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;

import java.util.List;

public class IntersectWithSmall<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> extends
  AbstractBinaryCollectionToCollectionOperator<VD, ED, GD> {
  @Override
  protected EPGraphCollection<VD, ED, GD> executeInternal(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondGraphCollection) throws Exception {
    final DataSet<Subgraph<Long, GD>> newSubgraphs =
      firstSubgraphs.union(secondSubgraphs)
        .groupBy(new KeySelectors.GraphKeySelector<GD>())
        .reduceGroup(new SubgraphGroupReducer<GD>(2));

    final List<Long> identifiers;
    identifiers =
      secondSubgraphs.map(new MapFunction<Subgraph<Long, GD>, Long>() {
        @Override
        public Long map(Subgraph<Long, GD> subgraph) throws Exception {
          return subgraph.getId();
        }
      }).collect();

    DataSet<Vertex<Long, VD>> vertices = firstGraph.getVertices();
    vertices = vertices.filter(new FilterFunction<Vertex<Long, VD>>() {

      @Override
      public boolean filter(Vertex<Long, VD> vertex) throws Exception {
        for (Long id : identifiers) {
          if (vertex.getValue().getGraphs().contains(id)) {
            return true;
          }
        }
        return false;
      }
    });

    DataSet<Edge<Long, ED>> edges = firstGraph.getEdges().join(vertices)
      .where(new KeySelectors.EdgeSourceVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>()).join(vertices)
      .where(new KeySelectors.EdgeTargetVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>());

    Graph<Long, VD, ED> newGraph = Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection<>(newGraph, newSubgraphs,
      firstCollection.getVertexDataFactory(),
      firstCollection.getEdgeDataFactory(),
      firstCollection.getGraphDataFactory(), env);
  }

  @Override
  public String getName() {
    return "IntersectWithSmall";
  }
}
