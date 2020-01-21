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
package org.gradoop.flink.model.impl.layouts.gve;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
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
public class GVEGraphLayoutFactory extends GVEBaseFactory
  implements LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> {

  @Override
  public GVELayout fromDataSets(DataSet<EPGMVertex> vertices) {
    return fromDataSets(vertices, createEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public GVELayout fromDataSets(DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges) {
    Objects.requireNonNull(vertices, "EPGMVertex DataSet was null");
    Objects.requireNonNull(edges, "EPGMEdge DataSet was null");
    EPGMGraphHead graphHead = getGraphHeadFactory().createGraphHead();

    DataSet<EPGMGraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
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
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets(
    DataSet<EPGMGraphHead> graphHead, DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges) {
    return create(graphHead, vertices, edges);
  }

  @Override
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromIndexedDataSets(
    Map<String, DataSet<EPGMVertex>> vertices, Map<String, DataSet<EPGMEdge>> edges) {
    EPGMGraphHead graphHead = getGraphHeadFactory().createGraphHead();

    DataSet<EPGMGraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    Map<String, DataSet<EPGMGraphHead>> graphHeads = Maps.newHashMap();
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
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromIndexedDataSets(
    Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices,
    Map<String, DataSet<EPGMEdge>> edges) {
    return create(graphHeads, vertices, edges);
  }

  @Override
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromCollections(
    EPGMGraphHead graphHead,
    Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {

    Objects.requireNonNull(vertices, "EPGMVertex collection was null");

    List<EPGMGraphHead> graphHeads;
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
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromCollections(
    Collection<EPGMVertex> vertices, Collection<EPGMEdge> edges) {
    Objects.requireNonNull(vertices, "EPGMVertex collection was null");
    Objects.requireNonNull(edges, "EPGMEdge collection was null");
    return fromDataSets(createVertexDataSet(vertices), createEdgeDataSet(edges));
  }

  @Override
  public LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> createEmptyGraph() {
    Collection<EPGMVertex> vertices = new ArrayList<>(0);
    Collection<EPGMEdge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges);
  }
}
