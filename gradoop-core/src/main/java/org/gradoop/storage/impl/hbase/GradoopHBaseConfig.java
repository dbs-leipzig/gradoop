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
import org.gradoop.config.GradoopStoreConfig;
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
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentEdgeFactory;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentGraphHeadFactory;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.PersistentVertexFactory;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.util.GConstants;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Configuration class for using HBase with Gradoop.
 *
 * @param <G>   EPGM graph head type
 * @param <V>   EPGM vertex type
 * @param <E>   EPGM edge type
 * @param <PG>  persistent graph head type
 * @param <PV>  persistent vertex type
 * @param <PE>  persistent edge type
 */
public class GradoopHBaseConfig<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  PG extends PersistentGraphHead,
  PV extends PersistentVertex<E>,
  PE extends PersistentEdge<V>>
  extends GradoopStoreConfig<G, V, E, PG, PV, PE> {

  /**
   * Graph table name.
   */
  private final String graphTableName;
  /**
   * Vertex table name.
   */
  private final String vertexTableName;

  /**
   * Edge table name.
   */
  private final String edgeTableName;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param persistentGraphHeadFactory  persistent graph head factory
   * @param persistentVertexFactory     persistent vertex factory
   * @param persistentEdgeFactory       persistent edge factory
   * @param graphTableName              graph table name
   * @param vertexTableName             vertex table name
   * @param edgeTableName               edge table name
   */
  private GradoopHBaseConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<V, E> edgeHandler,
    PersistentGraphHeadFactory<G, PG> persistentGraphHeadFactory,
    PersistentVertexFactory<V, E, PV> persistentVertexFactory,
    PersistentEdgeFactory<V, E, PE> persistentEdgeFactory,
    String graphTableName,
    String vertexTableName,
    String edgeTableName) {
    super(graphHeadHandler,
      vertexHandler,
      edgeHandler,
      persistentGraphHeadFactory,
      persistentVertexFactory,
      persistentEdgeFactory);
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "Vertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "Edge table name was null or empty");

    this.graphTableName = graphTableName;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop Store configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(GradoopStoreConfig<G, V, E, PG, PV, PE> config,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    this(config.getGraphHeadHandler(),
      config.getVertexHandler(),
      config.getEdgeHandler(),
      config.getPersistentGraphHeadFactory(),
      config.getPersistentVertexFactory(),
      config.getPersistentEdgeFactory(),
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
  public static GradoopHBaseConfig<
    GraphHeadPojo,
    VertexPojo,
    EdgePojo,
    HBaseGraphHead,
    HBaseVertex,
    HBaseEdge>
  getDefaultConfig() {
    GraphHeadHandler<GraphHeadPojo> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadPojoFactory());
    VertexHandler<VertexPojo, EdgePojo> vertexHandler =
      new HBaseVertexHandler<>(new VertexPojoFactory());
    EdgeHandler<VertexPojo, EdgePojo> edgeHandler =
      new HBaseEdgeHandler<>(new EdgePojoFactory());

    PersistentGraphHeadFactory<GraphHeadPojo, HBaseGraphHead>
      persistentGraphHeadFactory = new HBaseGraphHeadFactory();
    PersistentVertexFactory<VertexPojo, EdgePojo, HBaseVertex>
      persistentVertexFactory = new HBaseVertexFactory();
    PersistentEdgeFactory<VertexPojo, EdgePojo, HBaseEdge>
      persistentEdgeFactory = new HBaseEdgeFactory();

    return new GradoopHBaseConfig<>(
      graphHeadHandler,
      vertexHandler,
      edgeHandler,
      persistentGraphHeadFactory,
      persistentVertexFactory,
      persistentEdgeFactory,
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
   * @param <G>             EPGM graph head type
   * @param <V>             EPGM vertex type
   * @param <E>             EPGM edge type
   * @param <PG>            persistent graph head type
   * @param <PV>            persistent vertex type
   * @param <PE>            persistent edge type
   *
   * @return Gradoop HBase configuration
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    PG extends PersistentGraphHead,
    PV extends PersistentVertex<E>,
    PE extends PersistentEdge<V>>
  GradoopHBaseConfig<G, V, E, PG, PV, PE> createConfig(
    GradoopStoreConfig<G, V, E, PG, PV, PE> gradoopConfig,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    return new GradoopHBaseConfig<>(gradoopConfig,
      graphTableName,
      vertexTableName,
      edgeTableName);
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
