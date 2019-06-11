/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;

/**
 * Returns the first n (arbitrary) base graphs from a collection.
 *
 * Note that this operator uses broadcasting to distribute the relevant graph identifiers.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class Limit<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> {

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
  public GC execute(GC collection) {

    DataSet<G> graphHeads = collection.getGraphHeads().first(limit);

    DataSet<GradoopId> firstIds = graphHeads.map(new Id<>());

    DataSet<V> filteredVertices = collection.getVertices()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<E> filteredEdges = collection.getEdges()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return collection.getFactory()
      .fromDataSets(graphHeads, filteredVertices, filteredEdges);
  }
}
