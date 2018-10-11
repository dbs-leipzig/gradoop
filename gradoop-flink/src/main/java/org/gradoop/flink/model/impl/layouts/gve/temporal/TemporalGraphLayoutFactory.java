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
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * TODO: descriptions
 */
public class TemporalGraphLayoutFactory extends BaseFactory implements BaseLayoutFactory {

  public TemporalLayout fromDataSets(DataSet<TemporalVertex> vertices,
    DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(vertices, "Temporal vertex DataSet is null.");
    Objects.requireNonNull(edges, "Temporal edge DataSet is null.");

    TemporalGraphHead graphHead = TemporalGraphHead.createGraphHead();

    DataSet<TemporalGraphHead> graphHeadSet = getConfig().getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties;txFrom;txTo;validFrom;validTo");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties;txFrom;txTo;validFrom;validTo");

    return new TemporalLayout(graphHeadSet, vertices, edges);
  }

  public TemporalLayout fromDataSets(DataSet<TemporalGraphHead> graphHead,
    DataSet<TemporalVertex> vertices, DataSet<TemporalEdge> edges) {
    Objects.requireNonNull(graphHead, "Temporal graphHead DataSet is null.");
    Objects.requireNonNull(vertices, "Temporal vertex DataSet is null.");
    Objects.requireNonNull(edges, "Temporal edge DataSet is null.");
    return new TemporalLayout(graphHead, vertices, edges);
  }

  public TemporalLayout fromNonTemporalDataSets(DataSet<GraphHead> graphHead,
    DataSet<Vertex> vertices, DataSet<Edge> edges) {
    return new TemporalLayout(
      graphHead.map(TemporalGraphHead::fromNonTemporalGraphHead),
      vertices.map(TemporalVertex::fromNonTemporalVertex),
      edges.map(TemporalEdge::fromNonTemporalEdge));
  }

  public TemporalLayout fromCollections(TemporalGraphHead graphHead,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {
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

  public TemporalLayout fromCollections(Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    Objects.requireNonNull(vertices, "Temporal vertex collection is null.");

    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
    }
    return fromDataSets(createTemporalVertexDataSet(vertices), createTemporalEdgeDataSet(edges));
  }

}
