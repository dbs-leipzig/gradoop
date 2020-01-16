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
package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToBaseGraphCollectionOperator;
import org.gradoop.flink.model.impl.operators.selection.SelectionBase;

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
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SelectionBase<G, V, E, LG, GC>
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

    return selectVerticesAndEdges(collection, graphHeads);
  }
}
