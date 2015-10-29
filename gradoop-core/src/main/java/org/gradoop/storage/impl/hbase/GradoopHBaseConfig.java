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
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.pojo.DefaultEdgeData;
import org.gradoop.model.impl.pojo.DefaultEdgeDataFactory;
import org.gradoop.model.impl.pojo.DefaultGraphData;
import org.gradoop.model.impl.pojo.DefaultGraphDataFactory;
import org.gradoop.model.impl.pojo.DefaultVertexData;
import org.gradoop.model.impl.pojo.DefaultVertexDataFactory;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.VertexDataHandler;
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
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData> extends GradoopConfig<VD, ED, GD> {

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
  private GradoopHBaseConfig(VertexDataHandler<VD, ED> vertexHandler,
    EdgeDataHandler<ED, VD> edgeHandler,
    GraphDataHandler<GD> graphHeadHandler,
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
  public static GradoopHBaseConfig<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> getDefaultConfig() {
    VertexDataHandler<DefaultVertexData, DefaultEdgeData> vertexDataHandler =
      new DefaultVertexDataHandler<>(new DefaultVertexDataFactory());
    EdgeDataHandler<DefaultEdgeData, DefaultVertexData> edgeDataHandler =
      new DefaultEdgeDataHandler<>(new DefaultEdgeDataFactory());
    GraphDataHandler<DefaultGraphData> graphDataHandler =
      new DefaultGraphDataHandler<>(new DefaultGraphDataFactory());
    return new GradoopHBaseConfig<>(vertexDataHandler, edgeDataHandler,
      graphDataHandler, GConstants.DEFAULT_TABLE_VERTICES,
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
  public static <VD extends VertexData, ED extends EdgeData,
    GD extends GraphData> GradoopHBaseConfig<VD, ED, GD> createConfig(
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
