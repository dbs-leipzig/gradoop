/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.layouts.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * Base class for graph layout factories.
 */
public abstract class BaseFactory implements BaseLayoutFactory<GraphHead, Vertex, Edge> {

  /**
   * Gradoop Flink config
   */
  private GradoopFlinkConfig config;

  /**
   * Knows how to create {@link GraphHead}
   */
  private final GraphHeadFactory graphHeadFactory;

  /**
   * Knows how to create {@link Vertex}
   */
  private final VertexFactory vertexFactory;

  /**
   *  Knows how to create {@link Edge}
   */
  private final EdgeFactory edgeFactory;

  /**
   * Creates a new Configuration.
   */
  protected BaseFactory() {
    this.graphHeadFactory = new GraphHeadFactory();
    this.vertexFactory = new VertexFactory();
    this.edgeFactory = new EdgeFactory();
  }

  @Override
  public EPGMGraphHeadFactory<GraphHead> getGraphHeadFactory() {
    return graphHeadFactory;
  }

  @Override
  public EPGMVertexFactory<Vertex> getVertexFactory() {
    return vertexFactory;
  }

  @Override
  public EPGMEdgeFactory<Edge> getEdgeFactory() {
    return edgeFactory;
  }

  @Override
  public void setGradoopFlinkConfig(GradoopFlinkConfig config) {
    Objects.requireNonNull(config);
    this.config = config;
  }

  protected GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Creates a graph head dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param graphHeads  graph heads
   * @return graph head dataset
   */
  protected DataSet<GraphHead> createGraphHeadDataSet(Collection<GraphHead> graphHeads) {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataSet<GraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = env
        .fromElements(getGraphHeadFactory().createGraphHead())
        .filter(new False<>());
    } else {
      graphHeadSet =  env.fromCollection(graphHeads);
    }
    return graphHeadSet;
  }

  /**
   * Creates a vertex dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param vertices  vertex collection
   * @return vertex dataset
   */
  protected DataSet<Vertex> createVertexDataSet(Collection<Vertex> vertices) {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataSet<Vertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = env
        .fromElements(getVertexFactory().createVertex())
        .filter(new False<>());
    } else {
      vertexSet = env.fromCollection(vertices);
    }
    return vertexSet;
  }

  /**
   * Creates an edge dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param edges edge collection
   * @return edge dataset
   */
  protected DataSet<Edge> createEdgeDataSet(Collection<Edge> edges) {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataSet<Edge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = env
        .fromElements(getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<>());
    } else {
      edgeSet = env.fromCollection(edges);
    }
    return edgeSet;
  }
}
