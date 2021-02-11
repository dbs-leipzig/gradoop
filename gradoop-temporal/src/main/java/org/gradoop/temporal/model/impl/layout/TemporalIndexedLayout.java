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
package org.gradoop.temporal.model.impl.layout;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Map;

/**
 * Represents a temporal graph or graph collection using one dataset per label and graph instance:
 * <ol>
 *   <li>a Map<L, G> containing the graph label L as key and a graph head dataset G as value</li>
 *   <li>a Map<L, V> containing the vertex label L as key and a vertex dataset V as value</li>
 *   <li>a Map<L, E> containing the edge label L as key and a edge dataset E as value</li>
 * </ol>
 */
public class TemporalIndexedLayout extends TemporalGVELayout implements
  LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge>,
  GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  /**
   * Mapping from graph label to graph heads with that label.
   */
  private final Map<String, DataSet<TemporalGraphHead>> graphHeads;
  /**
   * Mapping from vertex label to vertices with that label.
   */
  private final Map<String, DataSet<TemporalVertex>> vertices;
  /**
   * Mapping from edge label to edges with that label.
   */
  private final Map<String, DataSet<TemporalEdge>> edges;

  /**
   * Creates a new temporal indexed layout.
   *
   * @param graphHeads mapping from label to graph heads
   * @param vertices mapping from label to vertices
   * @param edges mapping from label to edges
   */
  TemporalIndexedLayout(Map<String, DataSet<TemporalGraphHead>> graphHeads,
    Map<String, DataSet<TemporalVertex>> vertices,
    Map<String, DataSet<TemporalEdge>> edges) {
    super(
      graphHeads.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during graph head union")),
      vertices.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during vertex union")),
      edges.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during edge union"))
    );
    this.graphHeads = graphHeads;
    this.vertices = vertices;
    this.edges = edges;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return true;
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHeadsByLabel(String label) {
    if (!graphHeads.containsKey(label)) {
      throw new IllegalArgumentException("The given graph label [" + label +
        "] is not available in the (indexed) graph layout.");
    }
    return graphHeads.get(label);
  }

  @Override
  public DataSet<TemporalVertex> getVerticesByLabel(String label) {
    if (!vertices.containsKey(label)) {
      throw new IllegalArgumentException("The given vertex label [" + label +
        "] is not available in the (indexed) graph layout.");
    }
    return vertices.get(label);
  }

  @Override
  public DataSet<TemporalEdge> getEdgesByLabel(String label) {
    if (!edges.containsKey(label)) {
      throw new IllegalArgumentException("The given edge label [" + label +
        "] is not available in the (indexed) graph layout.");
    }
    return edges.get(label);
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHead() {
    return getGraphHeads();
  }

  /**
   * The request for all elements without specifying a label results in a union of all label-partitioned
   * datasets.
   *
   * @return the whole graph head dataset created by a union of all single datasets
   */
  @Override
  public DataSet<TemporalGraphHead> getGraphHeads() {
    return graphHeads.values().stream().reduce(DataSet::union)
      .orElseThrow(() -> new RuntimeException("Error during graph head union"));
  }

  /**
   * The request for all elements without specifying a label results in a union of all label-partitioned
   * datasets.
   *
   * @return the whole vertex dataset created by a union of all single datasets
   */
  @Override
  public DataSet<TemporalVertex> getVertices() {
    return vertices.values().stream().reduce(DataSet::union)
      .orElseThrow(() -> new RuntimeException("Error during vertex union"));
  }

  /**
   * The request for all elements without specifying a label results in a union of all label-partitioned
   * datasets.
   *
   * @return the whole edge dataset created by a union of all single datasets
   */
  @Override
  public DataSet<TemporalEdge> getEdges() {
    return edges.values().stream().reduce(DataSet::union)
      .orElseThrow(() -> new RuntimeException("Error during edge union."));
  }
}
