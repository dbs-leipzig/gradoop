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
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Factory responsible for creating temporal GVE graph layouts.
 */
public class TemporalGraphLayoutFactory extends TemporalBaseLayoutFactory
  implements LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalVertex> vertices) {
    return fromDataSets(vertices, createEdgeDataSet(Collections.emptyList()));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    requireNonNull(vertices, "Vertex DataSet is null");
    requireNonNull(edges, "Edge DataSet is null");
    TemporalGraphHead graphHead = getGraphHeadFactory().createGraphHead();

    DataSet<TemporalGraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties;transactionTime;validTime");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties;transactionTime;validTime");

    return new TemporalGVELayout(graphHeadSet, vertices, edges);
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalGraphHead> graphHead,
    DataSet<TemporalVertex> vertices,
    DataSet<TemporalEdge> edges) {
    requireNonNull(graphHead, "Temporal graphHead DataSet is null.");
    requireNonNull(vertices, "Temporal vertex DataSet is null.");
    requireNonNull(edges, "Temporal edge DataSet is null.");
    return new TemporalGVELayout(graphHead, vertices, edges);
  }

  /**
   * {@inheritDoc}
   *
   * Creating a temporal graph layout from an indexed dataset is not supported yet.
   */
  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromIndexedDataSets(
    Map<String, DataSet<TemporalVertex>> vertices, Map<String, DataSet<TemporalEdge>> edges) {
    throw new UnsupportedOperationException(
      "Creating a temporal graph layout from an indexed dataset is not supported yet.");
  }

  /**
   * {@inheritDoc}
   *
   * Creating a temporal graph layout from an indexed dataset is not supported yet.
   */
  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromIndexedDataSets(
    Map<String, DataSet<TemporalGraphHead>> graphHeads,
    Map<String, DataSet<TemporalVertex>> vertices, Map<String, DataSet<TemporalEdge>> edges) {
    throw new UnsupportedOperationException(
      "Creating a temporal graph layout from an indexed dataset is not supported yet.");
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromCollections(
    TemporalGraphHead graphHead, Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    requireNonNull(vertices, "Temporal vertex collection is null.");

    List<TemporalGraphHead> graphHeads = graphHead == null ? Collections.emptyList() :
      Collections.singletonList(graphHead);
    if (edges == null) {
      edges = Collections.emptyList();
    }

    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromCollections(
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    requireNonNull(vertices, "Temporal vertex collection is null.");
    requireNonNull(edges, "Temporal edge collection is null.");
    return fromDataSets(createVertexDataSet(vertices), createEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> createEmptyGraph() {
    return fromCollections(null, Collections.emptyList(), Collections.emptyList());
  }
}
