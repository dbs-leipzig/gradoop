/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.epgm.gve;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Responsible for producing GVE-based graph collections.
 */
public class GVELogicalGraphFactory extends GVEGraphBaseFactory implements LogicalGraphFactory {

  /**
   * Creates a new logical graph factory.
   *
   * @param config Gradoop config
   */
  public GVELogicalGraphFactory(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<Vertex> vertices) {
    return fromDataSets(vertices,
      GVEGraphBase.createEdgeDataSet(Lists.newArrayListWithCapacity(0), config));
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    Objects.requireNonNull(config, "Config was null");
    GraphHead graphHead = config
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<GraphHead> graphHeadSet = config.getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return new GVELogicalGraph(graphHeadSet, vertices, edges, config);
  }

  @Override
  public LogicalGraph fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new GVELogicalGraph(graphHead, vertices, edges, config);
  }

  @Override
  public LogicalGraph fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    List<GraphHead> graphHeads;
    if (graphHead == null) {
      graphHeads = Lists.newArrayListWithCapacity(0);
    } else {
      graphHeads = Lists.newArrayList(graphHead);
    }

    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
    }

    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    Objects.requireNonNull(config, "Config was null");

    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges)
    );
  }

  @Override
  public LogicalGraph fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    Objects.requireNonNull(config, "Config was null");

    GraphHead graphHead = config.getGraphHeadFactory().createGraphHead();

    DataSet<Vertex> vertexDataSet = createVertexDataSet(vertices)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");

    DataSet<Edge> edgeDataSet = createEdgeDataSet(edges)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return fromDataSets(
      createGraphHeadDataSet(new ArrayList<>(0)),
      vertexDataSet, edgeDataSet
    );
  }

  @Override
  public LogicalGraph createEmptyGraph() {
    Collection<Vertex> vertices = new ArrayList<>(0);
    Collection<Edge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges);
  }
}
