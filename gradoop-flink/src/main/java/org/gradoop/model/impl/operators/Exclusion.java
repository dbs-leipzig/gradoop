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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.GraphDataFactory;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.impl.EPGraph;

public class Exclusion<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> extends
  AbstractBinaryGraphToGraphOperator<VD, ED, GD> {

  @Override
  public String getName() {
    return "Exclusion";
  }

  @Override
  protected EPGraph<VD, ED, GD> executeInternal(EPGraph<VD, ED, GD> firstGraph,
    EPGraph<VD, ED, GD> secondGraph) {
    final Long newGraphID = FlinkConstants.EXCLUDE_GRAPH_ID;

    Graph<Long, VD, ED> graph1 = firstGraph.getGellyGraph();
    Graph<Long, VD, ED> graph2 = secondGraph.getGellyGraph();

    // union vertex sets, group by vertex id, filter vertices where the group
    // contains exactly one vertex which belongs to the graph, the operator is
    // called on
    DataSet<Vertex<Long, VD>> newVertexSet =
      graph1.getVertices().union(graph2.getVertices())
        .groupBy(new KeySelectors.VertexKeySelector<VD>()).reduceGroup(
        new VertexGroupReducer<VD>(1L, firstGraph.getId(), secondGraph.getId()))
        .map(new VertexToGraphUpdater<VD>(newGraphID));

    JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>> joinFunc =
      new JoinFunction<Edge<Long, ED>, Vertex<Long, VD>, Edge<Long, ED>>() {
        @Override
        public Edge<Long, ED> join(Edge<Long, ED> leftTuple,
          Vertex<Long, VD> rightTuple) throws Exception {
          return leftTuple;
        }
      };

    // In exclude(), we are only interested in edges that connect vertices
    // that are in the exclusion of the vertex sets. Thus, we join the edges
    // from the left graph with the new vertex set using source and target ids.
    DataSet<Edge<Long, ED>> newEdgeSet = graph1.getEdges().join(newVertexSet)
      .where(new KeySelectors.EdgeSourceVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>()).with(joinFunc)
      .join(newVertexSet)
      .where(new KeySelectors.EdgeTargetVertexKeySelector<ED>())
      .equalTo(new KeySelectors.VertexKeySelector<VD>()).with(joinFunc)
      .map(new EdgeToGraphUpdater<ED>(newGraphID));

    return EPGraph.fromGraph(
      Graph.fromDataSet(newVertexSet, newEdgeSet, graph1.getContext()),
      firstGraph.getGraphDataFactory().createGraphData(newGraphID),
      firstGraph.getVertexDataFactory(), firstGraph.getEdgeDataFactory(),
      firstGraph.getGraphDataFactory());
  }
}
