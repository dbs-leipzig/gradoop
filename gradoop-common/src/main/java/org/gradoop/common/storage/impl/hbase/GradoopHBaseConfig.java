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

package org.gradoop.common.storage.impl.hbase;

import org.apache.commons.lang.StringUtils;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.util.GConstants;
import org.gradoop.common.config.GradoopConfig;
import org.gradoop.common.config.GradoopStoreConfig;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.VertexHandler;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Configuration class for using HBase with Gradoop.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopHBaseConfig
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends GradoopStoreConfig<G, V, E> {

  /**
   * Graph table name.
   */
  private final String graphTableName;
  /**
   * EPGMVertex table name.
   */
  private final String vertexTableName;

  /**
   * EPGMEdge table name.
   */
  private final String edgeTableName;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param graphTableName              graph table name
   * @param vertexTableName             vertex table name
   * @param edgeTableName               edge table name
   */
  private GradoopHBaseConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<E, V> edgeHandler,
    String graphTableName,
    String vertexTableName,
    String edgeTableName) {
    super(graphHeadHandler,
      vertexHandler,
      edgeHandler,
      new HBaseGraphHeadFactory<G>(),
      new HBaseVertexFactory<V, E>(),
      new HBaseEdgeFactory<E, V>());
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "EPGMVertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "EPGMEdge table name was null or empty");

    this.graphTableName = graphTableName;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(GradoopConfig<G, V, E> config,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    this(config.getGraphHeadHandler(),
      config.getVertexHandler(),
      config.getEdgeHandler(),
      graphTableName,
      vertexTableName,
      edgeTableName);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @return Default Gradoop HBase configuration.
   */
  public static GradoopHBaseConfig<GraphHead, Vertex, Edge> getDefaultConfig() {
    GraphHeadHandler<GraphHead> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadFactory());
    VertexHandler<Vertex, Edge> vertexHandler =
      new HBaseVertexHandler<>(new VertexFactory());
    EdgeHandler<Edge, Vertex> edgeHandler =
      new HBaseEdgeHandler<>(new EdgeFactory());

    return new GradoopHBaseConfig<>(
      graphHeadHandler,
      vertexHandler,
      edgeHandler,
      GConstants.DEFAULT_TABLE_GRAPHS,
      GConstants.DEFAULT_TABLE_VERTICES,
      GConstants.DEFAULT_TABLE_EDGES);
  }

  /**
   * Creates a Gradoop HBase configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return Gradoop HBase configuration
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GradoopHBaseConfig<G, V, E> createConfig(GradoopConfig<G, V, E> gradoopConfig,
    String vertexTableName, String edgeTableName, String graphTableName) {
    return new GradoopHBaseConfig<>(gradoopConfig, graphTableName,
      vertexTableName, edgeTableName);
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
