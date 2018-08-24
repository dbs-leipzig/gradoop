/*
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
package org.gradoop.storage.config;

import org.apache.accumulo.core.security.Authorizations;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.storage.common.config.GradoopStoreConfig;
import org.gradoop.storage.impl.accumulo.constants.AccumuloDefault;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.storage.impl.accumulo.handler.AccumuloGraphHandler;
import org.gradoop.storage.impl.accumulo.handler.AccumuloVertexHandler;

import java.util.Properties;

/**
 * Gradoop Accumulo configuration define
 */
public class GradoopAccumuloConfig implements GradoopStoreConfig {

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
   * Definition for serialize version control
   */
  private static final int serialVersionUID = 23;

  /**
   * accumulo properties
   */
  private final Properties accumuloProperties = new Properties();

  /**
   * row handler for EPGM GraphHead
   */
  private final AccumuloGraphHandler graphHandler;

  /**
   * row handler for EPGM Vertex
   */
  private final AccumuloVertexHandler vertexHandler;

  /**
   * row handler for EPGM Edge
   */
  private final AccumuloEdgeHandler edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHandler                graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   */
  private GradoopAccumuloConfig(
    AccumuloGraphHandler graphHandler,
    AccumuloVertexHandler vertexHandler,
    AccumuloEdgeHandler edgeHandler
  ) {
    this.graphHandler = graphHandler;
    this.vertexHandler = vertexHandler;
    this.edgeHandler = edgeHandler;
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @return Default Gradoop Accumulo configuration.
   */
  public static GradoopAccumuloConfig getDefaultConfig() {
    GraphHeadFactory graphHeadFactory = new GraphHeadFactory();
    EdgeFactory edgeFactory = new EdgeFactory();
    VertexFactory vertexFactory = new VertexFactory();
    return new GradoopAccumuloConfig(
      new AccumuloGraphHandler(graphHeadFactory),
      new AccumuloVertexHandler(vertexFactory),
      new AccumuloEdgeHandler(edgeFactory));
  }

  /**
   * property setter
   *
   * @param key property key
   * @param value property value
   * @return configure itself
   */
  public GradoopAccumuloConfig set(String key, Object value) {
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
  public <T> T get(String key, T defValue) {
    Object value = accumuloProperties.get(key);
    if (value == null) {
      return defValue;
    } else {
      //noinspection unchecked
      return (T) value;
    }
  }

  /**
   * Get accumulo properties
   *
   * @return accumulo properties
   */
  public Properties getAccumuloProperties() {
    return accumuloProperties;
  }

  /**
   * Get graph handler
   *
   * @return graph handler
   */
  public AccumuloGraphHandler getGraphHandler() {
    return graphHandler;
  }

  /**
   * Get vertex handler
   *
   * @return vertex handler
   */
  public AccumuloVertexHandler getVertexHandler() {
    return vertexHandler;
  }

  /**
   * Get edge handler
   *
   * @return edge handler
   */
  public AccumuloEdgeHandler getEdgeHandler() {
    return edgeHandler;
  }

  /**
   * Get edge table name
   *
   * @return edge table name
   */
  public String getEdgeTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.EDGE;
  }

  /**
   * Get vertex table name
   *
   * @return vertex table name
   */
  public String getVertexTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.VERTEX;
  }

  /**
   * Get graph head table name
   *
   * @return graph head table name
   */
  public String getGraphHeadTable() {
    return get(ACCUMULO_TABLE_PREFIX, AccumuloDefault.TABLE_PREFIX) + AccumuloTables.GRAPH;
  }

  @Override
  public String toString() {
    return accumuloProperties.toString();
  }

}
