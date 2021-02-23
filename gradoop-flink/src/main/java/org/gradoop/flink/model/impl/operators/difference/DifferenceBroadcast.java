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
package org.gradoop.flink.model.impl.operators.difference;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns a collection with all base graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 * <p>
 * This operator implementation requires that a list of subgraph identifiers
 * in the resulting graph collections fits into the workers main memory.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class DifferenceBroadcast<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends Difference<G, V, E, LG, GC> {

  /**
   * Computes the resulting vertices by collecting a list of resulting
   * subgraphs and checking if the vertex is contained in that list.
   *
   * @param newGraphHeads graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<V> computeNewVertices(DataSet<G> newGraphHeads) {

    DataSet<GradoopId> identifiers = newGraphHeads
      .map(new Id<>());

    return firstCollection.getVertices()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(identifiers, GraphsContainmentFilterBroadcast.GRAPH_IDS);
  }
}
