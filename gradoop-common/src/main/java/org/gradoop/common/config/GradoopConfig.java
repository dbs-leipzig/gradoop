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

package org.gradoop.common.config;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.common.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.HBaseVertexHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic Gradoop Configuration.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopConfig
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {

  /**
   * Graph head handler.
   */
  private final GraphHeadHandler<G> graphHeadHandler;
  /**
   * EPGMVertex handler.
   */
  private final VertexHandler<V, E> vertexHandler;
  /**
   * EPGMEdge handler.
   */
  private final EdgeHandler<E, V> edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler  graph head handler
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   */
  protected GradoopConfig(GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler, EdgeHandler<E, V> edgeHandler) {
    this.graphHeadHandler =
      checkNotNull(graphHeadHandler, "GraphHeadHandler was null");
    this.vertexHandler =
      checkNotNull(vertexHandler, "VertexHandler was null");
    this.edgeHandler =
      checkNotNull(edgeHandler, "EdgeHandler was null");
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads.
   *
   * @return Default Gradoop configuration.
   */
  public static GradoopConfig<GraphHead, Vertex, Edge> getDefaultConfig() {
    VertexHandler<Vertex, Edge> vertexHandler = new HBaseVertexHandler<>(
      new VertexFactory());
    EdgeHandler<Edge, Vertex> edgeHandler = new HBaseEdgeHandler<>(
      new EdgeFactory());
    GraphHeadHandler<GraphHead> graphHeadHandler = new HBaseGraphHeadHandler<>(
      new GraphHeadFactory());
    return new GradoopConfig<>(graphHeadHandler, vertexHandler, edgeHandler);
  }

  /**
   * Creates a Gradoop configuration based on the given arguments.
   *
   * @param graphHeadHandler  EPGM graph head handler
   * @param vertexHandler     EPGM vertex handler
   * @param edgeHandler       EPGM edge handler
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return Gradoop HBase configuration
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GradoopConfig createConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<E, V> edgeHandler) {
    return new GradoopConfig<>(graphHeadHandler, vertexHandler, edgeHandler);
  }

  public GraphHeadHandler<G> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexHandler<V, E> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler<E, V> getEdgeHandler() {
    return edgeHandler;
  }

  public EPGMGraphHeadFactory<G> getGraphHeadFactory() {
    return graphHeadHandler.getGraphHeadFactory();
  }

  public EPGMVertexFactory<V> getVertexFactory() {
    return vertexHandler.getVertexFactory();
  }

  public EPGMEdgeFactory<E> getEdgeFactory() {
    return edgeHandler.getEdgeFactory();
  }
}
