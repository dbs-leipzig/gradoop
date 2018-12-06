/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.ElementFactoryProvider;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;

import java.util.Collection;

/**
 * Responsible for creating instances of graph collections with type {@link GC} based on a specific
 * {@link org.gradoop.flink.model.api.layouts.GraphCollectionLayout}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <GC> the type of the graph collection that will be created with this factory
 */
public interface BaseGraphCollectionFactory<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  GC extends BaseGraphCollection<G, V, E, GC>> extends ElementFactoryProvider<G, V, E> {

  /**
   * Sets the layout factory that is responsible for creating a graph collection layout.
   *
   * @param layoutFactory graph collection layout factory
   */
  void setLayoutFactory(GraphCollectionLayoutFactory<G, V, E> layoutFactory);

  /**
   * Creates a collection from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @return Graph collection
   */
  GC fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices);

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads  GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Graph collection
   */
  GC fromDataSets(DataSet<G> graphHeads, DataSet<V> vertices, DataSet<E> edges);

  /**
   * Creates a collection layout from the given collections.
   *
   * @param graphHeads  Graph Head collection
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Graph collection
   */
  GC fromCollections(Collection<G> graphHeads, Collection<V> vertices, Collection<E> edges);

  /**
   * Creates a graph collection from a given logical graph.
   *
   * @param logicalGraphLayout the graph layout with stored graph elements
   * @return 1-element graph collection
   */
  GC fromGraph(LogicalGraphLayout<G, V, E> logicalGraphLayout);

  /**
   * Creates a graph collection from multiple given logical graphs.
   *
   * @param logicalGraphLayout  input graphs
   * @return graph collection
   */
  GC fromGraphs(LogicalGraphLayout<G, V, E>... logicalGraphLayout);

  /**
   * Creates an empty graph collection.
   *
   * @return empty graph collection
   */
  GC createEmptyCollection();
}
