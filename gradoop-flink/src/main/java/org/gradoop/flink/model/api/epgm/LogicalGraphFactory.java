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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Responsible for create instances of {@link LogicalGraph} based on a specific
 * {@link org.gradoop.flink.model.api.layouts.LogicalGraphLayout}.
 */
public class LogicalGraphFactory {
  /**
   * Creates the layout from given data.
   */
  private LogicalGraphLayoutFactory layoutFactory;
  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new factory.
   *
   * @param config Configuration
   */
  public LogicalGraphFactory(GradoopFlinkConfig config) {
    this.config = config;
  }

  /**
   * Sets the layout factory that is responsible for creating a graph layout.
   *
   * @param layoutFactory graph layout factory
   */
  public void setLayoutFactory(LogicalGraphLayoutFactory layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
    this.layoutFactory.setGradoopFlinkConfig(config);
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param vertices  Vertex dataset
   * @return Logical graph
   */
  public LogicalGraph fromDataSets(DataSet<Vertex> vertices) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices), config);
  }

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
  public LogicalGraph fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices, edges), config);
  }

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
  public LogicalGraph fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(graphHead, vertices, edges), config);
  }

  /**
   * Creates a logical graph from the given datasets. A new graph head is created and all vertices
   * and edges are assigned to that graph head.
   *
   * @param vertices label indexed vertex datasets
   * @param edges label indexed edge datasets
   * @return Logical graph
   */
  public LogicalGraph fromIndexedDataSets(Map<String, DataSet<Vertex>> vertices,
    Map<String, DataSet<Edge>> edges) {
    return new LogicalGraph(layoutFactory.fromIndexedDataSets(vertices, edges), config);
  }

  /**
   * Creates a logical graph from the given datasets. The method assumes, that all vertices and
   * edges are already assigned to the specified graph head.
   *
   * @param graphHeads label indexed graph head dataset (1-element)
   * @param vertices label indexed vertex datasets
   * @param edges label indexed edge datasets
   * @return Logical graph
   */
  public LogicalGraph fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    return new LogicalGraph(layoutFactory.fromIndexedDataSets(graphHeads, vertices, edges), config);
  }

  /**
   * Creates a logical graph from the given arguments.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  public LogicalGraph fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(graphHead, vertices, edges), config);
  }

  /**
   * Creates a logical graph from the given arguments. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices    Vertex collection
   * @param edges       Edge collection
   * @return Logical graph
   */
  public LogicalGraph fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(vertices, edges), config);
  }

  /**
   * Creates an empty graph.
   *
   * @return empty graph
   */
  public LogicalGraph createEmptyGraph() {
    return new LogicalGraph(layoutFactory.createEmptyGraph(), config);
  }
}
