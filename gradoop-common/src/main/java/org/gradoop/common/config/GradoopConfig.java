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

import org.gradoop.common.model.api.epgm.EdgeFactory;
import org.gradoop.common.model.api.epgm.GraphHeadFactory;
import org.gradoop.common.model.api.epgm.VertexFactory;
import org.gradoop.common.model.impl.pojo.EdgePojoFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.common.model.impl.pojo.VertexPojoFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.common.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.HBaseVertexHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Basic Gradoop Configuration.
 */
public class GradoopConfig {

  /**
   * Graph head handler.
   */
  private final GraphHeadHandler graphHeadHandler;
  /**
   * Vertex handler.
   */
  private final VertexHandler vertexHandler;
  /**
   * Edge handler.
   */
  private final EdgeHandler edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler  graph head handler
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   */
  protected GradoopConfig(GraphHeadHandler graphHeadHandler,
    VertexHandler vertexHandler,
    EdgeHandler edgeHandler) {
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
  public static GradoopConfig getDefaultConfig() {
    VertexHandler vertexHandler =
      new HBaseVertexHandler(new VertexPojoFactory());
    EdgeHandler edgeHandler =
      new HBaseEdgeHandler(new EdgePojoFactory());
    GraphHeadHandler graphHeadHandler =
      new HBaseGraphHeadHandler(new GraphHeadPojoFactory());
    return new GradoopConfig(graphHeadHandler, vertexHandler, edgeHandler);
  }

  /**
   * Creates a Gradoop configuration based on the given arguments.
   *
   * @param graphHeadHandler  EPGM graph head handler
   * @param vertexHandler     EPGM vertex handler
   * @param edgeHandler       EPGM edge handler
   *
   * @return Gradoop HBase configuration
   */
  public static GradoopConfig createConfig(
    GraphHeadHandler graphHeadHandler,
    VertexHandler vertexHandler,
    EdgeHandler edgeHandler) {
    return new GradoopConfig(graphHeadHandler, vertexHandler, edgeHandler);
  }

  public GraphHeadHandler getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexHandler getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler getEdgeHandler() {
    return edgeHandler;
  }

  public GraphHeadFactory getGraphHeadFactory() {
    return graphHeadHandler.getGraphHeadFactory();
  }

  public VertexFactory getVertexFactory() {
    return vertexHandler.getVertexFactory();
  }

  public EdgeFactory getEdgeFactory() {
    return edgeHandler.getEdgeFactory();
  }
}
