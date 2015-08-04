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
import org.gradoop.model.impl.EPFlinkEdgeData;
import org.gradoop.model.impl.EPFlinkGraphData;
import org.gradoop.model.impl.EPFlinkVertexData;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.Subgraph;

import static org.gradoop.model.impl.EPGraph.*;

public class Union extends AbstractBinaryCollectionToCollectionOperator {
  @Override
  protected EPGraphCollection executeInternal(EPGraphCollection firstCollection,
    EPGraphCollection secondGraphCollection) {
    DataSet<Subgraph<Long, EPFlinkGraphData>> newSubgraphs =
      firstSubgraphs.union(secondSubgraphs).distinct(GRAPH_ID);
    DataSet<Vertex<Long, EPFlinkVertexData>> vertices =
      firstGraph.getVertices().union(secondGraph.getVertices())
        .distinct(VERTEX_ID);
    DataSet<Edge<Long, EPFlinkEdgeData>> edges =
      firstGraph.getEdges().union(secondGraph.getEdges()).distinct(EDGE_ID);
    return new EPGraphCollection(Graph.fromDataSet(vertices, edges, env),
      newSubgraphs, env);
  }

  @Override
  public String getName() {
    return "Union";
  }
}
