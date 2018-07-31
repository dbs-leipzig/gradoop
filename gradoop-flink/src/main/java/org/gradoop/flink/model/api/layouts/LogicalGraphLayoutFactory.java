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
package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;
import java.util.Map;

/**
 * Enables the construction of a {@link LogicalGraphLayout}.
 */
public interface LogicalGraphLayoutFactory extends BaseLayoutFactory {
  /**
   * Creates a logical graph layout from the given vertex dataset.
   *
   * @param vertices  Vertex dataset
   * @return Logical graph layout
   */
  LogicalGraphLayout fromDataSets(DataSet<Vertex> vertices);

  /**
   * Creates a logical graph layout from given vertex and edge datasets.
   *
   * The method creates a new graph head element and assigns the vertices and
   * edges to that graph.
   *
   * @param vertices Vertex DataSet
   * @param edges    Edge DataSet
   * @return Logical graph layout
   */
  LogicalGraphLayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges);

  /**
   * Creates a logical graph layout from given graphHead, vertex and edge datasets.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return Logical graph layout
   */
  LogicalGraphLayout fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges);

  /**
   * Creates a graph layout from the given datasets indexed by label.
   *
   * @param vertices Mapping from label to vertex dataset
   * @param edges Mapping from label to edge dataset
   * @return Logical graph layout
   */
  LogicalGraphLayout fromIndexedDataSets(Map<String, DataSet<Vertex>> vertices,
    Map<String, DataSet<Edge>> edges);

  /**
   * Creates a graph layout from the given datasets indexed by label.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHeads 1-element Mapping from label to GraphHead DataSet
   * @param vertices Mapping from label to vertex dataset
   * @param edges Mapping from label to edge dataset
   * @return Logical graph layout
   */
  LogicalGraphLayout fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges);

  /**
   * Creates a logical graph layout from the given graphHead, vertex and edge collections.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph layout
   */
  LogicalGraphLayout fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges);

  /**
   * Creates a logical graph layout from the given vertex and edge collections. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Logical graph layout
   */
  LogicalGraphLayout fromCollections(Collection<Vertex> vertices, Collection<Edge> edges);

  /**
   * Creates a layout representing the empty graph.
   *
   * @return empty graph
   */
  LogicalGraphLayout createEmptyGraph();
}
