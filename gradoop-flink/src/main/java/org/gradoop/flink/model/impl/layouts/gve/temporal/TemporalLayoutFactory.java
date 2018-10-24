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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalEdgeFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalGraphHeadFromNonTemporal;
import org.gradoop.flink.model.impl.functions.tpgm.TemporalVertexFromNonTemporal;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.Objects;

/**
 * Factory class responsible to create a {@link TemporalGVELayout} from given datasets or
 * collections.
 */
public class TemporalLayoutFactory extends BaseFactory implements BaseLayoutFactory {

  /**
   * Create a temporal graph layout representing a logical graph by vertices and edges. A new graph
   * head will be created and assigned to the given elements.
   *
   * @param vertices the temporal vertices
   * @param edges the temporal edges
   * @return a temporal graph layout representing the temporal graph data
   */
  public TemporalGVELayout fromDataSets(DataSet<TemporalVertex> vertices,
    DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(vertices, "Temporal vertex DataSet is null.");
    Objects.requireNonNull(edges, "Temporal edge DataSet is null.");

    TemporalGraphHead graphHead = TemporalGraphHead.createGraphHead();

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

  /**
   * Creates a temporal graph layout from given temporal graph head, vertex and edge datasets.
   *
   * The method assumes that the given vertices and edges are already assigned to the given
   * graph head.
   *
   * @param graphHead   1-element GraphHead DataSet
   * @param vertices    Vertex DataSet
   * @param edges       Edge DataSet
   * @return a temporal graph layout representing the temporal graph data
   */
  public TemporalGVELayout fromDataSets(DataSet<TemporalGraphHead> graphHead,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(graphHead, "Temporal graphHead DataSet is null.");
    Objects.requireNonNull(vertices, "Temporal vertex DataSet is null.");
    Objects.requireNonNull(edges, "Temporal edge DataSet is null.");
    return new TemporalGVELayout(graphHead, vertices, edges);
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
