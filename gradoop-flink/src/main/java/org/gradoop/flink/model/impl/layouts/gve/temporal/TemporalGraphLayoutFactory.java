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
package org.gradoop.flink.model.impl.layouts.gve.temporal;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalEdgeFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalGraphHeadFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalVertexFromNonTemporal;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Factory class responsible for creating {@link TemporalGVELayout} from given datasets or
 * collections.
 */
public class TemporalGraphLayoutFactory extends BaseFactory implements
  LogicalGraphLayoutFactory<TemporalGraphHead, TemporalVertex, TemporalEdge> {

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalVertex> vertices) {
    return fromDataSets(vertices, createTemporalEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(vertices, "Vertex DataSet is null");
    Objects.requireNonNull(edges, "Edge DataSet is null");
    TemporalGraphHead graphHead = TemporalGraphHead.createGraphHead();

    DataSet<TemporalGraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return new TemporalGVELayout(graphHeadSet, vertices, edges);
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromDataSets(
    DataSet<TemporalGraphHead> graphHead,
    DataSet<TemporalVertex> vertices,
    DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(graphHead, "Temporal graphHead DataSet is null.");
    Objects.requireNonNull(vertices, "Temporal vertex DataSet is null.");
    Objects.requireNonNull(edges, "Temporal edge DataSet is null.");
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
    Objects.requireNonNull(vertices, "Temporal vertex collection is null.");

    List<TemporalGraphHead> graphHeads;
    if (graphHead == null) {
      graphHeads = Lists.newArrayListWithCapacity(0);
    } else {
      graphHeads = Lists.newArrayList(graphHead);
    }
    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
    }

    return fromDataSets(
      createTemporalGraphHeadDataSet(graphHeads),
      createTemporalVertexDataSet(vertices),
      createTemporalEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> fromCollections(
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
    Objects.requireNonNull(vertices, "Temporal vertex collection is null.");
    Objects.requireNonNull(edges, "Temporal edge collection is null.");
    return fromDataSets(createTemporalVertexDataSet(vertices), createTemporalEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> createEmptyGraph() {
    Collection<TemporalVertex> vertices = new ArrayList<>(0);
    Collection<TemporalEdge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges);
  }

  /**
   * Creates a temporal graph layout from given non-temporal EPGM graph head, vertex and edge
   * datasets.
   *
   * The method assumes that the given vertices and edges are already assigned to the given
   * graph head.
   *
   * @param graphHead   1-element EPGM graph head DataSet
   * @param vertices    EPGM vertex DataSet
   * @param edges       EPGM edge DataSet
   * @return a temporal graph layout representing the temporal graph data
   */
  public TemporalGVELayout fromNonTemporalDataSets(DataSet<GraphHead> graphHead,
    DataSet<Vertex> vertices, DataSet<Edge> edges) {
    return new TemporalGVELayout(
      graphHead.map(new TemporalGraphHeadFromNonTemporal()),
      vertices.map(new TemporalVertexFromNonTemporal()),
      edges.map(new TemporalEdgeFromNonTemporal()));
  }
}
