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

package org.gradoop.storage.impl.hbase;

import org.apache.commons.lang.StringUtils;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.util.GConstants;
import org.gradoop.util.GradoopConfig;

/**
 * Configuration class for using HBase with Gradoop.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class GradoopHBaseConfig<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead> extends GradoopConfig<VD, ED, GD> {

  /**
   * Vertex table name.
   */
  private final String vertexTableName;

  /**
   * Edge table name.
   */
  private final String edgeTableName;

  /**
   * Graph table name.
   */
  private final String graphTableName;

  /**
   * Creates a new Configuration.
   *
   * @param vertexHandler    vertex handler
   * @param edgeHandler      edge handler
   * @param graphHeadHandler graph head handler
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   * @param graphTableName  graph table name
   *
   */
  private GradoopHBaseConfig(VertexHandler<VD, ED> vertexHandler,
    EdgeHandler<ED, VD> edgeHandler,
    GraphHeadHandler<GD> graphHeadHandler,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    super(vertexHandler, edgeHandler, graphHeadHandler);
    if (StringUtils.isEmpty(vertexTableName)) {
      throw new IllegalArgumentException(
        "Vertex table name must not be null or empty");
    }
    if (StringUtils.isEmpty(edgeTableName)) {
      throw new IllegalArgumentException(
        "Edge table name must not be null or empty");
    }
    if (StringUtils.isEmpty(graphTableName)) {
      throw new IllegalArgumentException(
        "Graph table name must not be null or empty");
    }
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
    this.graphTableName = graphTableName;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   * @param graphTableName  graph table name
   */
  private GradoopHBaseConfig(GradoopConfig<VD, ED, GD> config,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    this(config.getVertexHandler(), config.getEdgeHandler(),
      config.getGraphHeadHandler(), vertexTableName, edgeTableName,
      graphTableName);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @return Default Gradoop HBase configuration.
   */
  public static GradoopHBaseConfig<VertexPojo, EdgePojo, GraphHeadPojo>
  getDefaultConfig() {
    VertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    EdgeHandler<EdgePojo, VertexPojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());
    GraphHeadHandler<GraphHeadPojo> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    return new GradoopHBaseConfig<>(vertexHandler, edgeHandler,
      graphHeadHandler, GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_EDGES, GConstants.DEFAULT_TABLE_GRAPHS);
  }

  /**
   * Creates a Gradoop HBase configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   * @param graphTableName  graph table name
   * @param <VD>            EPGM vertex type
   * @param <ED>            EPGM edge type
   * @param <GD>            EPGM graph head type
   *
   * @return Gradoop HBase configuration
   */
  public static <VD extends EPGMVertex, ED extends EPGMEdge,
    GD extends EPGMGraphHead> GradoopHBaseConfig<VD, ED, GD> createConfig(
    GradoopConfig<VD, ED, GD> gradoopConfig, String vertexTableName,
    String edgeTableName, String graphTableName) {
    return new GradoopHBaseConfig<>(gradoopConfig, vertexTableName,
      edgeTableName, graphTableName);
  }

  public String getVertexTableName() {
    return vertexTableName;
  }

  public String getEdgeTableName() {
    return edgeTableName;
  }

  public String getGraphTableName() {
    return graphTableName;
  }
}
