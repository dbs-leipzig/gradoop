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

package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.operators
  .UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAllGraphsBroadcast;


import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the first n (arbitrary) logical graphs from a collection.
 *
 * Note that this operator uses broadcasting to distribute the relevant graph
 * identifiers.
 */
public class Limit implements UnaryCollectionToCollectionOperator {

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
  public GraphCollection execute(GraphCollection collection) {

    DataSet<EPGMGraphHead> graphHeads = collection.getGraphHeads().first(limit);

    DataSet<GradoopId> firstIds = graphHeads.map(new Id<EPGMGraphHead>());

    DataSet<EPGMVertex> filteredVertices = collection.getVertices()
      .filter(new InAllGraphsBroadcast<EPGMVertex>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<EPGMEdge> filteredEdges = collection.getEdges()
      .filter(new InAllGraphsBroadcast<EPGMEdge>())
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
