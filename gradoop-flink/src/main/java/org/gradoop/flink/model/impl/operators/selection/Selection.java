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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

import java.util.Objects;

/**
 * Filter base graphs from a graph collection based on their associated graph head.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class Selection<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SelectionBase<G, V, E, LG, GC> {

  /**
   * User-defined predicate function
   */
  private final FilterFunction<G> predicate;

  /**
   * Creates a new Selection operator.
   *
   * @param predicate user-defined predicate function
   */
  public Selection(FilterFunction<G> predicate) {
    this.predicate = Objects.requireNonNull(predicate, "Predicate function was null");
  }

  @Override
  public GC execute(GC collection) {
    DataSet<G> graphHeads = collection.getGraphHeads().filter(predicate);
    return selectVerticesAndEdges(collection, graphHeads);
  }
}
