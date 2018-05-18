/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.common.config;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentEdgeFactory;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.api.PersistentVertexFactory;
import org.gradoop.common.storage.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.HBaseEdgeFactory;
import org.gradoop.common.storage.impl.hbase.HBaseEdgeHandler;
import org.gradoop.common.storage.impl.hbase.HBaseGraphHeadFactory;
import org.gradoop.common.storage.impl.hbase.HBaseGraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.HBaseVertexFactory;
import org.gradoop.common.storage.impl.hbase.HBaseVertexHandler;
import org.gradoop.common.util.HBaseConstants;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration class for using HBase with Gradoop.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GradoopHBaseConfig<G extends EPGMGraphHead, V extends EPGMVertex, E extends
  EPGMEdge> extends
  GradoopStoreConfig<PersistentGraphHeadFactory<G>, PersistentVertexFactory<V, E>,
    PersistentEdgeFactory<E, V>> {


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
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param env                                   flink execution environment
   * @param graphTableName              graph table name
   * @param vertexTableName             vertex table name
   * @param edgeTableName               edge table name
   */
  private GradoopHBaseConfig(
    GraphHeadHandler<G> graphHeadHandler,
    VertexHandler<V, E> vertexHandler,
    EdgeHandler<E, V> edgeHandler,
    ExecutionEnvironment env,
    String graphTableName,
    String vertexTableName,
    String edgeTableName) {
    super(new HBaseGraphHeadFactory<G>(),
      new HBaseVertexFactory<V, E>(),
      new HBaseEdgeFactory<E, V>(),
      env);
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "EPGMVertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "EPGMEdge table name was null or empty");

    this.graphTableName = graphTableName;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;

    this.graphHeadHandler =
        checkNotNull(graphHeadHandler, "GraphHeadHandler was null");
    this.vertexHandler =
        checkNotNull(vertexHandler, "VertexHandler was null");
    this.edgeHandler =
        checkNotNull(edgeHandler, "EdgeHandler was null");
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(GradoopHBaseConfig<G, V, E> config,
    String vertexTableName,
    String edgeTableName,
    String graphTableName) {
    this(config.getGraphHeadHandler(),
      config.getVertexHandler(),
      config.getEdgeHandler(),
      config.getExecutionEnvironment(),
      graphTableName,
      vertexTableName,
      edgeTableName);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   *@param env apache flink execution environment
   * @return Default Gradoop HBase configuration.
   */
  public static GradoopHBaseConfig<GraphHead, Vertex, Edge>
    getDefaultConfig(ExecutionEnvironment env) {
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
      env,
      HBaseConstants.DEFAULT_TABLE_GRAPHS,
      HBaseConstants.DEFAULT_TABLE_VERTICES,
      HBaseConstants.DEFAULT_TABLE_EDGES);
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
  GradoopHBaseConfig<G, V, E> createConfig(GradoopHBaseConfig<G, V, E> gradoopConfig,
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

  public GraphHeadHandler<G> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexHandler<V, E> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler<E, V> getEdgeHandler() {
    return edgeHandler;
  }
}
