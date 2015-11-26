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
package org.gradoop.model.impl.operators.collection.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToGraphOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.filterfunctions
  .EdgeInAllGraphsFilterWithBC;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInAllGraphsFilterWithBC;
import org.gradoop.model.impl.functions.isolation.ElementIdOnly;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Intersects all subgraphs of a GraphCollection and returns the result
 * as new logical graph.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <G> EPGM graph head type
 */
public class OverlapCollection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToGraphOperator<G, V, E>{

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(GraphCollection<G, V, E> collection) {

    DataSet<G> graphHeads = collection.getGraphHeads();

    DataSet<GradoopId> graphIDs = graphHeads.map(new ElementIdOnly<G>());

    DataSet<V> vertices = collection.getVertices()
      .filter(new VertexInAllGraphsFilterWithBC<V>())
      .withBroadcastSet(graphIDs, VertexInAllGraphsFilterWithBC.BC_IDENTIFIERS);

    DataSet<E> edges = collection.getEdges()
      .filter(new EdgeInAllGraphsFilterWithBC<E>())
      .withBroadcastSet(graphIDs, EdgeInAllGraphsFilterWithBC.BC_IDENTIFIERS);

    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return OverlapCollection.class.getName();
  }
}
