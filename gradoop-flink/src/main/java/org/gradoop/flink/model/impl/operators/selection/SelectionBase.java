/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;

/**
 * Superclass of selection and distinct operators.
 * Contains logic of vertex and edge selection and updating.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public abstract class SelectionBase<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphCollectionToBaseGraphCollectionOperator<GC> {

  @Override
  public abstract GC execute(GC collection);

  /**
   * Selects vertices and edges for a selected subset of graph heads / graph ids.
   * Creates a graph collection representing selection result.
   *
   * @param collection input collection
   * @param graphHeads selected graph heads
   *
   * @return selection result
   */
  protected GC selectVerticesAndEdges(GC collection, DataSet<G> graphHeads) {

    // get the identifiers of these base graphs
    DataSet<GradoopId> graphIds = graphHeads.map(new Id<>());

    // use graph ids to filter vertices from the actual graph structure
    DataSet<V> vertices = collection.getVertices()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<E> edges = collection.getEdges()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return collection.getFactory().fromDataSets(graphHeads, vertices, edges);
  }
}
