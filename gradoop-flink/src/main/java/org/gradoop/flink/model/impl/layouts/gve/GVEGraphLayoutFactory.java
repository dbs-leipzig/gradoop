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
package org.gradoop.flink.model.impl.layouts.gve;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Responsible for creating a {@link GVELayout} from given data.
 */
public class GVEGraphLayoutFactory extends GVEBaseFactory implements LogicalGraphLayoutFactory {

  @Override
  public GVELayout fromDataSets(DataSet<Vertex> vertices) {
    return fromDataSets(vertices,
      createEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public GVELayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    GraphHead graphHead = getConfig()
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<GraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return new GVELayout(graphHeadSet, vertices, edges);
  }

  @Override
  public LogicalGraphLayout fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return create(graphHead, vertices, edges);
  }

  @Override
  public LogicalGraphLayout fromIndexedDataSets(Map<String, DataSet<Vertex>> vertices,
    Map<String, DataSet<Edge>> edges) {
    GraphHead graphHead = getConfig()
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<GraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    Map<String, DataSet<GraphHead>> graphHeads = Maps.newHashMap();
    graphHeads.put(graphHead.getLabel(), graphHeadSet);

    // update vertices and edges with new graph head id
    vertices = vertices.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey, e -> e.getValue().map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;label;properties")));

    edges = edges.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey, e -> e.getValue().map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;sourceId;targetId;label;properties")));

    return create(graphHeads, vertices, edges);
  }

  @Override
  public LogicalGraphLayout fromIndexedDataSets(Map<String, DataSet<GraphHead>> graphHeads,
    Map<String, DataSet<Vertex>> vertices, Map<String, DataSet<Edge>> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public LogicalGraphLayout fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {

    Objects.requireNonNull(vertices, "Vertex collection was null");

    List<GraphHead> graphHeads;
    if (graphHead == null) {
      graphHeads = Lists.newArrayListWithCapacity(0);
    } else {
      graphHeads = Lists.newArrayList(graphHead);
    }
    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
    }

    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    return fromDataSets(createVertexDataSet(vertices), createEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout createEmptyGraph() {
    Collection<Vertex> vertices = new ArrayList<>(0);
    Collection<Edge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges);
  }
}
