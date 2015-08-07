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
import org.gradoop.model.helper.KeySelectors;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;

public class Union<VD extends VertexData, ED extends EdgeData, GD extends
  GraphData> extends
  AbstractBinaryCollectionToCollectionOperator<VD, ED, GD> {

  @Override
  protected EPGraphCollection<VD, ED, GD> executeInternal(
    EPGraphCollection<VD, ED, GD> firstCollection,
    EPGraphCollection<VD, ED, GD> secondGraphCollection) {
    DataSet<Subgraph<Long, GD>> newSubgraphs =
      firstSubgraphs.union(secondSubgraphs)
        .distinct(new KeySelectors.GraphKeySelector<GD>());
    DataSet<Vertex<Long, VD>> vertices =
      firstGraph.getVertices().union(secondGraph.getVertices())
        .distinct(new KeySelectors.VertexKeySelector<VD>());
    DataSet<Edge<Long, ED>> edges =
      firstGraph.getEdges().union(secondGraph.getEdges())
        .distinct(new KeySelectors.EdgeKeySelector<ED>());

    return new EPGraphCollection<>(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, firstCollection.getVertexDataFactory(),
      firstCollection.getEdgeDataFactory(),
      firstCollection.getGraphDataFactory(), env);
  }

  @Override
  public String getName() {
    return "Union";
  }
}
