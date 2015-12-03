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

/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.config;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.storage.impl.hbase.HBaseVertexHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic Gradoop Configuration.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopConfig<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> {

  /**
   * Graph head handler.
   */
  private final GraphHeadHandler<G> graphHeadHandler;
  /**
   * Vertex handler.
   */
  private final VertexHandler<V, E> vertexHandler;
  /**
   * Edge handler.
   */
  private final EdgeHandler<V, E> edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler  graph head handler
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   */
  protected GradoopConfig(GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler) {
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
  public static GradoopConfig<GraphHeadPojo, VertexPojo, EdgePojo>
  getDefaultConfig() {
    VertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    EdgeHandler<VertexPojo, EdgePojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());
    GraphHeadHandler<GraphHeadPojo> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    return new GradoopConfig<>(graphHeadHandler, vertexHandler, edgeHandler);
  }

  /**
   * Creates a Gradoop configuration based on the given arguments.
   *
   * @param graphHeadHandler  EPGM graph head handler
   * @param vertexHandler     EPGM vertex handler
   * @param edgeHandler       EPGM edge handler
   * @param <G>               EPGM graph head type
   * @param <V>               EPGM vertex type
   * @param <E>               EPGM edge type
   *
   * @return Gradoop HBase configuration
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> GradoopConfig<G, V, E> createConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler) {
    return new GradoopConfig<>(graphHeadHandler, vertexHandler, edgeHandler);
  }

  public GraphHeadHandler<G> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexHandler<V, E> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler<V, E> getEdgeHandler() {
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
