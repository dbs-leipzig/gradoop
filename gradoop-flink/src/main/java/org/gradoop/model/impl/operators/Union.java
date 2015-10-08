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
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.KeySelectors;
import org.gradoop.model.impl.tuples.Subgraph;

/**
 * Returns a collection with all logical graphs from two input collections.
 * Graph equality is based on their identifiers.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class Union<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> extends
  SetOperator<VD, ED, GD> {

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Vertex<Long, VD>> computeNewVertices(
    DataSet<Subgraph<Long, GD>> newSubgraphs) throws Exception {
    return firstGraph.getVertices().union(secondGraph.getVertices())
      .distinct(new KeySelectors.VertexKeySelector<VD>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Subgraph<Long, GD>> computeNewSubgraphs() {
    return firstSubgraphs.union(secondSubgraphs)
      .distinct(new KeySelectors.GraphKeySelector<GD>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected DataSet<Edge<Long, ED>> computeNewEdges(
    DataSet<Vertex<Long, VD>> newVertices) {
    return firstGraph.getEdges().union(secondGraph.getEdges())
      .distinct(new KeySelectors.EdgeKeySelector<ED>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Union.class.getName();
  }
}
