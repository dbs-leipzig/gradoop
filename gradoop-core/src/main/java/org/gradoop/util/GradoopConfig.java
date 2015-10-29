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

import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.VertexDataHandler;

/**
 * Basic Gradoop Configuration.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public abstract class GradoopConfig<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData> {

  /**
   * Vertex handler.
   */
  private final VertexDataHandler<VD, ED> vertexHandler;
  /**
   * Edge handler.
   */
  private final EdgeDataHandler<ED, VD> edgeHandler;
  /**
   * Graph head handler.
   */
  private final GraphDataHandler<GD> graphHeadHandler;

  /**
   * Creates a new Configuration.
   *
   * @param vertexHandler     vertex handler
   * @param edgeHandler       edge handler
   * @param graphHeadHandler  graph head handler
   */
  public GradoopConfig(VertexDataHandler<VD, ED> vertexHandler,
    EdgeDataHandler<ED, VD> edgeHandler,
    GraphDataHandler<GD> graphHeadHandler) {
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

  public VertexDataHandler<VD, ED> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeDataHandler<ED, VD> getEdgeHandler() {
    return edgeHandler;
  }

  public GraphDataHandler<GD> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexDataFactory<VD> getVertexFactory() {
    return vertexHandler.getVertexDataFactory();
  }

  public EdgeDataFactory<ED> getEdgeFactory() {
    return edgeHandler.getEdgeDataFactory();
  }

  public GraphDataFactory<GD> getGraphHeadFactory() {
    return graphHeadHandler.getGraphDataFactory();
  }
}
