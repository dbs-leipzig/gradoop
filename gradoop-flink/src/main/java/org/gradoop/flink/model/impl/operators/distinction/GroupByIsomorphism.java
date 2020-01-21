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
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.functions.GraphHeadReduceFunction;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.distinction.functions.GraphHeadGroup;
import org.gradoop.flink.model.impl.operators.selection.SelectionBase;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * Returns a distinct collection of base graphs. Graphs are compared by isomorphism testing.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class GroupByIsomorphism<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends SelectionBase<G, V, E, LG, GC> {

  /**
   * Distinction function.
   */
  private final GraphHeadReduceFunction<G> function;

  /**
   * Constructor.
   *
   * @param function reduce function to merge multiple heads of isomorphic graphs
   */
  public GroupByIsomorphism(GraphHeadReduceFunction<G> function) {
    this.function = function;
  }

  @Override
  public GC execute(GC collection) {
    DataSet<G> graphHeads = getCanonicalLabels(collection)
      .join(collection.getGraphHeads())
      .where(0).equalTo(new Id<>())
      .with(new GraphHeadGroup<>())
      .groupBy(0)
      .reduceGroup(function);

    return selectVerticesAndEdges(collection, graphHeads);
  }

  /**
   * Creates a canonical label for each graph in a collection.
   *
   * @param collection input collection
   * @return graph head label set
   */
  DataSet<GraphHeadString> getCanonicalLabels(GC collection) {
    // Init builder for canonical labels
    CanonicalAdjacencyMatrixBuilder<G, V, E, LG, GC> camBuilder =
      new CanonicalAdjacencyMatrixBuilder<>(
      new GraphHeadToEmptyString<>(),  new VertexToDataString<>(), new EdgeToDataString<>(), true);

    // create canonical labels for all graph heads and choose representative for all distinct ones
    return camBuilder
      .getGraphHeadStrings(collection);
  }
}
