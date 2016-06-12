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

package org.gradoop.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphBaseOperators;
import org.gradoop.model.impl.functions.bool.False;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

/**
 * Base class for graph representations.
 *
 * @param <G> graph data type
 * @param <V> vertex data type
 * @param <E> edge data type
 * @see LogicalGraph
 * @see GraphCollection
 */
public abstract class GraphBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphBaseOperators<G, V, E> {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  protected final DataSet<G> graphHeads;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * DataSet containing vertices associated with that graph.
   */
  private final DataSet<V> vertices;
  /**
   * DataSet containing edges associated with that graph.
   */
  private final DataSet<E> edges;

  /**
   * Creates a new graph instance.
   *
   * @param graphHeads  graph head data set
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param config      Gradoop Flink configuration
   */
  protected GraphBase(DataSet<G> graphHeads, DataSet<V> vertices,
    DataSet<E> edges, GradoopFlinkConfig<G, V, E> config) {
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
   * @param <G>         graph head type
   * @param <V>         vertex type
   * @param <E>         edge type
   * @return graph head dataset
   */
  protected static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> DataSet<G>
  createGraphHeadDataSet(
    Collection<G> graphHeads, GradoopFlinkConfig<G, V, E> config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<G> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = config.getExecutionEnvironment()
        .fromElements(config.getGraphHeadFactory().createGraphHead())
        .filter(new False<G>());
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
   * @param <G>       graph head type
   * @param <V>       vertex type
   * @param <E>       edge type
   * @return vertex dataset
   */
  protected static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> DataSet<V>
  createVertexDataSet(
    Collection<V> vertices, GradoopFlinkConfig<G, V, E> config) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<V> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = config.getExecutionEnvironment()
        .fromElements(config.getVertexFactory().createVertex())
        .filter(new False<V>());
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
   * @param <G> graph head type
   * @param <V> vertex type
   * @param <E> edge type
   * @return edge dataset
   */
  protected static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> DataSet<E>
  createEdgeDataSet(
    Collection<E> edges, GradoopFlinkConfig<G, V, E> config) {

    DataSet<E> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = config.getExecutionEnvironment()
        .fromElements(config.getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<E>());
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
  public GradoopFlinkConfig<G, V, E> getConfig() {
    return config;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<V> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<E> getEdges() {
    return edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<E> getOutgoingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<E>() {
        @Override
        public boolean filter(E edge) throws Exception {
          return edge.getSourceId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<E> getIncomingEdges(final GradoopId vertexID) {
    return
      this.edges.filter(new FilterFunction<E>() {
        @Override
        public boolean filter(E edge) throws Exception {
          return edge.getTargetId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph<GradoopId, V, E> toGellyGraph() {
    return Graph.fromDataSet(getVertices().map(
      new MapFunction<V, Vertex<GradoopId, V>>() {
        @Override
        public Vertex<GradoopId, V> map(V epgmVertex) throws Exception {
          return new Vertex<>(epgmVertex.getId(), epgmVertex);
        }
      }).withForwardedFields("*->f1"),
      getEdges().map(new MapFunction<E, Edge<GradoopId, E>>() {
        @Override
        public Edge<GradoopId, E> map(E epgmEdge) throws Exception {
          return new Edge<>(epgmEdge.getSourceId(),
            epgmEdge.getTargetId(),
            epgmEdge);
        }
      }).withForwardedFields("*->f2"),
      config.getExecutionEnvironment());
  }
}
