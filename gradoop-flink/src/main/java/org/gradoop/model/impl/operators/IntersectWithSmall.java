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
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;

import java.util.List;

import static org.gradoop.model.impl.EPGraph.*;

public class IntersectWithSmall extends
  AbstractBinaryCollectionToCollectionOperator {
  @Override
  protected EPGraphCollection executeInternal(EPGraphCollection firstCollection,
    EPGraphCollection secondGraphCollection) throws Exception {
    final DataSet<Subgraph<Long, GraphData>> newSubgraphs =
      firstSubgraphs.union(secondSubgraphs).groupBy(GRAPH_ID)
        .reduceGroup(new SubgraphGroupReducer(2));

    final List<Long> identifiers;
    identifiers =
      secondSubgraphs.map(new MapFunction<Subgraph<Long, GraphData>, Long>() {
        @Override
        public Long map(Subgraph<Long, GraphData> subgraph) throws Exception {
          return subgraph.getId();
        }
      }).collect();

    DataSet<Vertex<Long, VertexData>> vertices = firstGraph.getVertices();
    vertices = vertices.filter(new FilterFunction<Vertex<Long, VertexData>>() {

      @Override
      public boolean filter(Vertex<Long, VertexData> vertex) throws Exception {
        for (Long id : identifiers) {
          if (vertex.getValue().getGraphs().contains(id)) {
            return true;
          }
        }
        return false;
      }
    });

    DataSet<Edge<Long, EdgeData>> edges = firstGraph.getEdges();

    edges = edges.join(vertices).where(SOURCE_VERTEX_ID).equalTo(VERTEX_ID)
      .with(new EdgeJoinFunction()).join(vertices).where(TARGET_VERTEX_ID)
      .equalTo(VERTEX_ID).with(new EdgeJoinFunction());

    Graph<Long, VertexData, EdgeData> newGraph =
      Graph.fromDataSet(vertices, edges, env);

    return new EPGraphCollection(newGraph, newSubgraphs, env);
  }

  @Override
  public String getName() {
    return "IntersectWithSmall";
  }
}
