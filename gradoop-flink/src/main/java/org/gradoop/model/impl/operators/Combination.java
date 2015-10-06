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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.helper.FlinkConstants;
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.impl.LogicalGraph;

/**
 * Creates a new logical graph by combining the vertex and edge sets of two
 * input graphs. Vertex and edge equality is based on their
 * respective identifiers.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class Combination<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> extends
  AbstractBinaryGraphToGraphOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected LogicalGraph<VD, ED, GD> executeInternal(
    LogicalGraph<VD, ED, GD> firstGraph, LogicalGraph<VD, ED, GD> secondGraph) {
    final Long newGraphID = FlinkConstants.COMBINE_GRAPH_ID;

    // build distinct union of vertex sets and update graph ids at vertices
    // cannot use Gelly union here because of missing argument for KeySelector
    DataSet<Vertex<Long, VD>> newVertexSet =
      firstGraph.getVertices().union(secondGraph.getVertices())
        .distinct(new KeySelectors.VertexKeySelector<VD>())
        .map(new VertexToGraphUpdater<VD>(newGraphID));

    DataSet<Edge<Long, ED>> newEdgeSet =
      firstGraph.getEdges().union(secondGraph.getEdges())
        .distinct(new KeySelectors.EdgeKeySelector<ED>())
        .map(new EdgeToGraphUpdater<ED>(newGraphID));

    return LogicalGraph.fromGellyGraph(Graph
        .fromDataSet(newVertexSet, newEdgeSet,
          newVertexSet.getExecutionEnvironment()),
      firstGraph.getGraphDataFactory().createGraphData(newGraphID),
      firstGraph.getVertexDataFactory(), firstGraph.getEdgeDataFactory(),
      firstGraph.getGraphDataFactory());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return "Combination";
  }
}
