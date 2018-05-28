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

import org.apache.accumulo.core.security.Authorizations;
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
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloDefault;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloGraphHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloVertexHandler;
import org.gradoop.common.storage.impl.accumulo.row.EdgeRow;
import org.gradoop.common.storage.impl.accumulo.row.GraphHeadRow;
import org.gradoop.common.storage.impl.accumulo.row.VertexRow;

import java.util.Properties;

/**
 * Gradoop Accumulo configuration define
 *
 * @param <G> graph head as output
 * @param <V> vertex as output
 * @param <E> edge as output
 */
public class GradoopAccumuloConfig<G extends EPGMGraphHead, V extends EPGMVertex, E extends
  EPGMEdge> extends
  GradoopStoreConfig<GraphHeadFactory, VertexFactory, EdgeFactory> {

  /**
   * accumulo user for accumulo connector, default "root"
   */
  public static final String ACCUMULO_USER = "accumulo.user";

  /**
   * accumulo password for accumulo connector, default empty
   */
  public static final String ACCUMULO_PASSWD = "accumulo.password";

  /**
   * accumulo instance name, default "gradoop"
   */
  public static final String ACCUMULO_INSTANCE = "accumulo.instance";

  /**
   * accumulo authorizations, default {@link Authorizations#EMPTY}
   */
  public static final String ACCUMULO_AUTHORIZATIONS = "accumulo.authorizations";

  /**
   * accumulo table prefix, you can define namespace and store prefix here
   */
  public static final String ACCUMULO_TABLE_PREFIX = "accumulo.table.prefix";

  /**
   * gradoop accumulo iterator priority, default 0xf
   */
  public static final String GRADOOP_ITERATOR_PRIORITY = "gradoop.iterator.priority";

  /**
   * gradoop batch scanner threads, default 10
   */
  public static final String GRADOOP_BATCH_SCANNER_THREADS = "gradoop.batch.scanner.threads";

  /**
   * zookeeper hosts, default "localhost:2181"
   */
  public static final String ZOOKEEPER_HOSTS = "zookeeper.hosts";

  /**
   * define for serialize version control
   */
  private static final int serialVersionUID = 23;

  /**
   * accumulo properties
   */
  private final Properties accumuloProperties = new Properties();

  /**
   * row handler for EPGM GraphHead
   */
  private transient AccumuloRowHandler<G, EPGMGraphHead, GraphHeadRow> graphHandler;

  /**
   * row handler for EPGM Vertex
   */
  private transient AccumuloRowHandler<V, EPGMVertex, VertexRow> vertexHandler;

  /**
   * row handler for EPGM Edge
   */
  private transient AccumuloRowHandler<E, EPGMEdge, EdgeRow> edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHandler                graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param env                         flink execution environment
   */
  private GradoopAccumuloConfig(
    AccumuloRowHandler<G, EPGMGraphHead, GraphHeadRow> graphHandler,
    AccumuloRowHandler<V, EPGMVertex, VertexRow> vertexHandler,
    AccumuloRowHandler<E, EPGMEdge, EdgeRow> edgeHandler,
    ExecutionEnvironment env
  ) {
    super(new GraphHeadFactory(), new VertexFactory(), new EdgeFactory(), env);
    this.graphHandler = graphHandler;
    this.vertexHandler = vertexHandler;
    this.edgeHandler = edgeHandler;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   */
  private GradoopAccumuloConfig(GradoopAccumuloConfig<G, V, E> config) {
    this(config.graphHandler, config.vertexHandler, config.edgeHandler,
      config.getExecutionEnvironment());
    this.accumuloProperties.putAll(config.accumuloProperties);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @param env apache flink execution environment
   * @return Default Gradoop Accumulo configuration.
   */
  public static GradoopAccumuloConfig<GraphHead, Vertex, Edge> getDefaultConfig(
    ExecutionEnvironment env
  ) {
    GraphHeadFactory graphHeadFactory = new GraphHeadFactory();
    EdgeFactory edgeFactory = new EdgeFactory();
    VertexFactory vertexFactory = new VertexFactory();
    return new GradoopAccumuloConfig<>(new AccumuloGraphHandler<>(graphHeadFactory),
      new AccumuloVertexHandler<>(vertexFactory), new AccumuloEdgeHandler<>(edgeFactory), env);
  }

  /**
   * Creates a Gradoop Accumulo configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   *
   * @return Gradoop HBase configuration
   */
  public static <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  GradoopAccumuloConfig<G, V, E> createConfig(
    GradoopAccumuloConfig<G, V, E> gradoopConfig
  ) {
    return new GradoopAccumuloConfig<>(gradoopConfig);
  }

  /**
   * property setter
   *
   * @param key property key
   * @param value property value
   * @return configure itself
   */
  public GradoopAccumuloConfig<G, V, E> set(
    String key,
    Object value
  ) {
    accumuloProperties.put(key, value);
    return this;
  }

  /**
   * integer value by key
   *
   * @param key property key
   * @param defValue default value
   * @param <T> value template
   * @return integer value
   */
  public <T> T get(
    String key,
    T defValue
  ) {
    Object value = accumuloProperties.get(key);
    if (value == null) {
      return defValue;
    } else {
      //noinspection unchecked
      return (T) value;
    }
  }

  public Properties getAccumuloProperties() {
    return accumuloProperties;
  }

  public AccumuloRowHandler<G, EPGMGraphHead, GraphHeadRow> getGraphHandler() {
    return graphHandler;
  }

  public AccumuloRowHandler<V, EPGMVertex, VertexRow> getVertexHandler() {
    return vertexHandler;
  }

  public AccumuloRowHandler<E, EPGMEdge, EdgeRow> getEdgeHandler() {
    return edgeHandler;
  }

  /**
   * get edge table name
   *
   * @return edge table name
   */
  public String getEdgeTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.EDGE;
  }

  /**
   * get vertex table name
   *
   * @return vertex table name
   */
  public String getVertexTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.VERTEX;
  }

  /**
   * get graph head table name
   *
   * @return graph head table name
   */
  public String getGraphHeadTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.GRAPH;
  }

}
