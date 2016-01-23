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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.limit;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Returns the first n (arbitrary) logical graphs from a collection.
 *
 * Note that this operator uses broadcasting to distribute the relevant graph
 * identifiers.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Limit
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * Number of graphs that are retrieved from the collection.
   */
  private final int limit;

  /**
   * Creates a new limit operator instance.
   *
   * @param limit number of graphs to retrieve from the collection
   */
  public Limit(int limit) {
    this.limit = limit;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection) {

    DataSet<G> graphHeads = collection.getGraphHeads().first(limit);

    DataSet<GradoopId> firstIds = graphHeads.map(new Id<G>());

    DataSet<V> filteredVertices = collection.getVertices()
      .filter(new InAllGraphsBroadcast<V>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<E> filteredEdges = collection.getEdges()
      .filter(new InAllGraphsBroadcast<E>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return GraphCollection.fromDataSets(graphHeads,
      filteredVertices,
      filteredEdges,
      collection.getConfig());
  }

  @Override
  public String getName() {
    return Limit.class.getName();
  }
}
