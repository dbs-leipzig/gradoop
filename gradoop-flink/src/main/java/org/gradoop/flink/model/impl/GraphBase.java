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
package org.gradoop.flink.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.GraphBaseOperators;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * Base class for graph representations.
 *
 * @see LogicalGraph
 * @see GraphCollection
 */
public abstract class GraphBase implements GraphBaseOperators {
  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;
  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<GraphHead> graphHeads;
  /**
   * DataSet containing vertices associated with that graph.
   */
  private final DataSet<Vertex> vertices;
  /**
   * DataSet containing edges associated with that graph.
   */
  private final DataSet<Edge> edges;

  /**
   * Creates a new graph instance.
   *
   * @param graphHeads  graph head data set
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param config      Gradoop Flink configuration
   */
  protected GraphBase(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config) {
    this.graphHeads = graphHeads;
    this.vertices = vertices;
    this.edges = edges;
    this.config = config;
  }

  /**
   * Creates a graph head dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param graphHeads  graph heads
   * @param config      configuration
   * @return graph head dataset
   */
  protected static DataSet<GraphHead> createGraphHeadDataSet(
    Collection<GraphHead> graphHeads, GradoopFlinkConfig config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<GraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = config.getExecutionEnvironment()
        .fromElements(config.getGraphHeadFactory().createGraphHead())
        .filter(new False<GraphHead>());
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
   * @param config    configuration
   * @return vertex dataset
   */
  protected static DataSet<Vertex> createVertexDataSet(
    Collection<Vertex> vertices, GradoopFlinkConfig config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<Vertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = config.getExecutionEnvironment()
        .fromElements(config.getVertexFactory().createVertex())
        .filter(new False<Vertex>());
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
   * @param config configuration
   * @return edge dataset
   */
  protected static DataSet<Edge> createEdgeDataSet(Collection<Edge> edges,
    GradoopFlinkConfig config) {

    DataSet<Edge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = config.getExecutionEnvironment()
        .fromElements(config.getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<Edge>());
    } else {
      edgeSet = config.getExecutionEnvironment().fromCollection(edges);
    }
    return edgeSet;
  }

  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return Gradoop Flink configuration
   */
  public GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Vertex> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge> getEdges() {
    return edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge> getOutgoingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<Edge>() {
        @Override
        public boolean filter(Edge edge) throws Exception {
          return edge.getSourceId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge> getIncomingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<Edge>() {
        @Override
        public boolean filter(Edge edge) throws Exception {
          return edge.getTargetId().equals(vertexID);
        }
      });
  }

  /**
   * Returns the graphHeads associated with that graph / graph collection.
   *
   * @return graph heads
   */
  protected DataSet<GraphHead> getGraphHeads() {
    return this.graphHeads;
  }
}
