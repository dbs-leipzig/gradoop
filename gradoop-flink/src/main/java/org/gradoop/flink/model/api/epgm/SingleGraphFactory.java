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
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.Collection;
import java.util.Map;

/**
 * Factory interface responsible for create instances of logical graphs with type {@link LG}
 * based on a specific {@link org.gradoop.flink.model.api.layouts.LogicalGraphLayout}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph that will be created with this factory
 */
public interface SingleGraphFactory<G extends EPGMGraphHead, V extends EPGMVertex,
  E extends EPGMEdge, LG extends SingleGraph> {

  /**
   * Sets the layout factory that is responsible for creating a graph layout.
   *
   * @param layoutFactory graph layout factory
   */
  void setLayoutFactory(LogicalGraphLayoutFactory<G, V, E> layoutFactory);

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param vertices  Vertex dataset
   * @return Logical graph
   */
  LG fromDataSets(DataSet<V> vertices);

  /**
   * Creates a logical graph from the given argument.
   *
   * The method creates a new graph head element and assigns the vertices and
   * edges to that graph.
   *
   * @param vertices Vertex DataSet
   * @param edges    Edge DataSet
   * @return Logical graph
   */
  LG fromDataSets(DataSet<V> vertices, DataSet<E> edges);

  /**
   * Creates a logical graph from the given arguments.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Logical graph
   */
  LogicalGraph fromDataSets(DataSet<G> graphHead, DataSet<V> vertices, DataSet<E> edges);

  /**
   * Creates a logical graph from the given datasets. A new graph head is created and all vertices
   * and edges are assigned to that graph head.
   *
   * @param vertices label indexed vertex datasets
   * @param edges label indexed edge datasets
   * @return Logical graph
   */
  LG fromIndexedDataSets(Map<String, DataSet<V>> vertices,
    Map<String, DataSet<E>> edges);

  /**
   * Creates a logical graph from the given datasets. The method assumes, that all vertices and
   * edges are already assigned to the specified graph head.
   *
   * @param graphHeads label indexed graph head dataset (1-element)
   * @param vertices label indexed vertex datasets
   * @param edges label indexed edge datasets
   * @return Logical graph
   */
  LG fromIndexedDataSets(Map<String, DataSet<G>> graphHeads,
    Map<String, DataSet<V>> vertices, Map<String, DataSet<E>> edges);

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  LG fromCollections(G graphHead, Collection<V> vertices, Collection<E> edges);

  /**
   * Creates a logical graph from the given arguments. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Logical graph
   */
  LG fromCollections(Collection<V> vertices, Collection<E> edges);

  /**
   * Creates an empty graph.
   *
   * @return empty graph
   */
  LG createEmptyGraph();
}
