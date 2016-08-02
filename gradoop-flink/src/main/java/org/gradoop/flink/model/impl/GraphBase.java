/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
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
   * Graph data associated with the logical graphs in that collection.
   */
  protected final DataSet<EPGMGraphHead> graphHeads;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * DataSet containing vertices associated with that graph.
   */
  private final DataSet<EPGMVertex> vertices;
  /**
   * DataSet containing edges associated with that graph.
   */
  private final DataSet<EPGMEdge> edges;

  /**
   * Creates a new graph instance.
   *
   * @param graphHeads  graph head data set
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param config      Gradoop Flink configuration
   */
  protected GraphBase(DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges, GradoopFlinkConfig config) {
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
  protected static DataSet<EPGMGraphHead> createGraphHeadDataSet(
    Collection<EPGMGraphHead> graphHeads, GradoopFlinkConfig config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<EPGMGraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = config.getExecutionEnvironment()
        .fromElements(config.getGraphHeadFactory().createGraphHead())
        .filter(new False<EPGMGraphHead>());
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
  protected static DataSet<EPGMVertex> createVertexDataSet(
    Collection<EPGMVertex> vertices, GradoopFlinkConfig config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<EPGMVertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = config.getExecutionEnvironment()
        .fromElements(config.getVertexFactory().createVertex())
        .filter(new False<EPGMVertex>());
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
  protected static DataSet<EPGMEdge> createEdgeDataSet(Collection<EPGMEdge> edges,
    GradoopFlinkConfig config) {

    DataSet<EPGMEdge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = config.getExecutionEnvironment()
        .fromElements(config.getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<EPGMEdge>());
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
  public DataSet<EPGMVertex> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<EPGMEdge> getEdges() {
    return edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<EPGMEdge> getOutgoingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<EPGMEdge>() {
        @Override
        public boolean filter(EPGMEdge edge) throws Exception {
          return edge.getSourceId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<EPGMEdge> getIncomingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<EPGMEdge>() {
        @Override
        public boolean filter(EPGMEdge edge) throws Exception {
          return edge.getTargetId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph<GradoopId, EPGMVertex, EPGMEdge> toGellyGraph() {
    return Graph.fromDataSet(getVertices().map(
      new MapFunction<EPGMVertex, org.apache.flink.graph.Vertex<GradoopId, EPGMVertex>>() {
        @Override
        public org.apache.flink.graph.Vertex<GradoopId, EPGMVertex> map(EPGMVertex epgmVertex) throws Exception {
          return new org.apache.flink.graph.Vertex<>(epgmVertex.getId(), epgmVertex);
        }
      }).withForwardedFields("*->f1"),
      getEdges().map(new MapFunction<EPGMEdge, org.apache.flink.graph.Edge<GradoopId, EPGMEdge>>() {
        @Override
        public org.apache.flink.graph.Edge<GradoopId, EPGMEdge> map(EPGMEdge epgmEdge) throws Exception {
          return new org.apache.flink.graph.Edge<>(epgmEdge.getSourceId(),
            epgmEdge.getTargetId(),
            epgmEdge);
        }
      }).withForwardedFields("*->f2"),
      config.getExecutionEnvironment());
  }
}
