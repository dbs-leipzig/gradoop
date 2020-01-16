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
package org.gradoop.flink.model.impl.operators.exclusion;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.ReducibleBinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.ByDifferentId;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;

/**
 * Computes the exclusion graph from a collection of logical graphs.
 * Reduces the starting graph to contain only vertices and edges that are not contained in any
 * other graph that is part of the given collection.
 * The graph head of the starting graph is retained.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class ReduceExclusion<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements ReducibleBinaryBaseGraphToBaseGraphOperator<GC, LG> {

  /**
   * Graph identifier to start excluding from in a collection scenario.
   */
  private final GradoopId startId;

  /**
   * Creates an operator instance which can be applied on a graph collection. As
   * exclusion is not a commutative operation, a start graph needs to be set
   * from which the remaining graphs will be excluded.
   *
   * @param startId graph id from which other graphs will be exluded from
   */
  public ReduceExclusion(GradoopId startId) {
    this.startId = startId;
  }

  @Override
  public LG execute(GC collection) {
    DataSet<GradoopId> excludedGraphIds = collection.getGraphHeads()
      .filter(new ByDifferentId<>(startId))
      .map(new Id<>());

    DataSet<V> vertices = collection.getVertices()
      .filter(new InGraph<>(startId))
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    DataSet<E> edges = collection.getEdges()
      .filter(new InGraph<>(startId))
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(excludedGraphIds, NotInGraphsBroadcast.GRAPH_IDS);

    return collection.getGraphFactory()
      .fromDataSets(collection.getGraphHeads().filter(new BySameId<>(startId)), vertices, edges);
  }
}
