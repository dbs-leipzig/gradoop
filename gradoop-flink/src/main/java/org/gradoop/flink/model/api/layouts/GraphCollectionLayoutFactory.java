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
package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;

/**
 * Enables the construction of a {@link GraphCollectionLayout}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public interface GraphCollectionLayoutFactory<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> extends BaseLayoutFactory {
  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads GraphHead DataSet
   * @param vertices Vertex DataSet
   * @return Graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices);

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads GraphHead DataSet
   * @param vertices Vertex DataSet
   * @param edges Edge DataSet
   * @return Graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices,
    DataSet<E> edges);

  /**
   * Creates a collection layout from the given datasets indexed by label.
   *
   * @param graphHeads Mapping from label to graph head dataset
   * @param vertices Mapping from label to vertex dataset
   * @param edges Mapping from label to edge dataset
   * @return Graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromIndexedDataSets(Map<String, DataSet<G>> graphHeads,
    Map<String, DataSet<V>> vertices, Map<String, DataSet<E>> edges);

  /**
   * Creates a collection layout from the given collections.
   *
   * @param graphHeads Graph Head collection
   * @param vertices Vertex collection
   * @param edges Edge collection
   * @return Graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromCollections(Collection<G> graphHeads,
    Collection<V> vertices, Collection<E> edges);

  /**
   * Creates a graph collection layout from a given logical graph layout.
   *
   * @param logicalGraphLayout input graph
   * @return graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromGraphLayout(LogicalGraphLayout<G, V, E> logicalGraphLayout);

  /**
   * Creates a graph collection layout from a graph transaction dataset.
   *
   * Overlapping vertices and edge are merged by Id comparison only.
   *
   * @param transactions transaction dataset
   * @return graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromTransactions(DataSet<GraphTransaction> transactions);

  /**
   * Creates a graph collection layout from graph transactions.
   *
   * Overlapping vertices and edge are merged using provided reduce functions.
   *
   * @param transactions  transaction dataset
   * @param vertexMergeReducer vertex merge function
   * @param edgeMergeReducer edge merge function
   * @return graph collection layout
   */
  GraphCollectionLayout<G, V, E> fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<V, V> vertexMergeReducer,
    GroupReduceFunction<E, E> edgeMergeReducer);

  /**
   * Creates an empty graph collection layout.
   *
   * @return empty graph collection layout
   */
  GraphCollectionLayout<G, V, E> createEmptyCollection();
}
