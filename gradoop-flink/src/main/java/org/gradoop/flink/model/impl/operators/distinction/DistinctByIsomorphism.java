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
package org.gradoop.flink.model.impl.operators.distinction;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.IdInBroadcast;
import org.gradoop.flink.model.impl.operators.distinction.functions.FirstGraphHead;
import org.gradoop.flink.model.impl.operators.distinction.functions.IdFromGraphHeadString;

/**
 * Returns a distinct collection of base graphs. Graphs are compared by isomorphism testing.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class DistinctByIsomorphism<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends GroupByIsomorphism<G, V, E, LG, GC> {

  /**
   * Default constructor.
   */
  public DistinctByIsomorphism() {
    super(new FirstGraphHead<>());
  }

  @Override
  public GC execute(GC collection) {
    // create canonical labels for all graph heads and choose representative for all distinct ones
    DataSet<GradoopId> graphIds = getCanonicalLabels(collection)
      .distinct(1)
      .map(new IdFromGraphHeadString());

    DataSet<G> graphHeads = collection.getGraphHeads()
      .filter(new IdInBroadcast<>())
      .withBroadcastSet(graphIds, IdInBroadcast.IDS);

    return selectVerticesAndEdges(collection, graphHeads);
  }
}
