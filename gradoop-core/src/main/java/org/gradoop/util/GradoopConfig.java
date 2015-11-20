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

package org.gradoop.util;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdgeFactory;
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

/**
 * Basic Gradoop Configuration.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class GradoopConfig<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead> {

  /**
   * Vertex handler.
   */
  private final VertexHandler<VD, ED> vertexHandler;
  /**
   * Edge handler.
   */
  private final EdgeHandler<ED, VD> edgeHandler;
  /**
   * Graph head handler.
   */
  private final GraphHeadHandler<GD> graphHeadHandler;

  /**
   * Creates a new Configuration.
   *
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   * @param graphHeadHandler  graph head handler
   */
  protected GradoopConfig(VertexHandler<VD, ED> vertexHandler,
    EdgeHandler<ED, VD> edgeHandler,
    GraphHeadHandler<GD> graphHeadHandler) {
    if (vertexHandler == null) {
      throw new IllegalArgumentException("Vertex handler must not be null");
    }
    if (edgeHandler == null) {
      throw new IllegalArgumentException("Edge handler must not be null");
    }
    if (graphHeadHandler == null) {
      throw new IllegalArgumentException("Graph head handler must not be null");
    }
    this.vertexHandler = vertexHandler;
    this.edgeHandler = edgeHandler;
    this.graphHeadHandler = graphHeadHandler;
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads.
   *
   * @return Default Gradoop configuration.
   */
  public static GradoopConfig<VertexPojo, EdgePojo, GraphHeadPojo>
  getDefaultConfig() {
    VertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    EdgeHandler<EdgePojo, VertexPojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());
    GraphHeadHandler<GraphHeadPojo> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    return new GradoopConfig<>(vertexHandler, edgeHandler,
      graphHeadHandler);
  }

  /**
   * Creates a Gradoop configuration based on the given arguments.
   *
   * @param vertexHandler     EPGM vertex handler
   * @param edgeHandler       EPGM edge handler
   * @param graphHeadHandler  EPGM graph head handler
   * @param <VD>              EPGM vertex type
   * @param <ED>              EPGM edge type
   * @param <GD>              EPGM graph head type
   *
   * @return Gradoop HBase configuration
   */
  public static <VD extends EPGMVertex, ED extends EPGMEdge,
    GD extends EPGMGraphHead> GradoopConfig<VD, ED, GD> createConfig(
    VertexHandler<VD, ED> vertexHandler,
    EdgeHandler<ED, VD> edgeHandler,
    GraphHeadHandler<GD> graphHeadHandler) {
    return new GradoopConfig<>(vertexHandler, edgeHandler, graphHeadHandler);
  }

  public VertexHandler<VD, ED> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler<ED, VD> getEdgeHandler() {
    return edgeHandler;
  }

  public GraphHeadHandler<GD> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public EPGMVertexFactory<VD> getVertexFactory() {
    return vertexHandler.getVertexFactory();
  }

  public EPGMEdgeFactory<ED> getEdgeFactory() {
    return edgeHandler.getEdgeFactory();
  }

  public EPGMGraphHeadFactory<GD> getGraphHeadFactory() {
    return graphHeadHandler.getGraphHeadFactory();
  }
}
