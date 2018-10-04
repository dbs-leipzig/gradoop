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

import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.storage.common.config.GradoopStoreConfig;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.storage.impl.accumulo.handler.AccumuloGraphHandler;
import org.gradoop.storage.impl.accumulo.handler.AccumuloVertexHandler;

import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gradoop Accumulo configuration define
 */
public class GradoopAccumuloConfig implements GradoopStoreConfig {

  /**
   * Definition for serialize version control
   */
  private static final int serialVersionUID = 23;

  /**
   * Accumulo client properties
   */
  private final Properties accumuloProperties = new Properties();

  /**
   * Row handler for EPGM GraphHead
   */
  private final AccumuloGraphHandler graphHandler;

  /**
   * Row handler for EPGM Vertex
   */
  private final AccumuloVertexHandler vertexHandler;

  /**
   * Row handler for EPGM Edge
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
   * Property setter
   *
   * @param key property key
   * @param value property value
   * @return configure itself
   */
  public GradoopAccumuloConfig set(GradoopAccumuloProperty key, Object value) {
    accumuloProperties.put(key.getKey(), value);
    return this;
  }

  /**
   * Get property value
   *
   * @param key property key
   * @param defValue default value
   * @param <T> value template
   * @return integer value
   */
  public <T> T get(GradoopAccumuloProperty key, T defValue) {
    Object value = accumuloProperties.get(key.getKey());
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
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.EDGE);
  }

  /**
   * Get vertex table name
   *
   * @return vertex table name
   */
  public String getVertexTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.VERTEX);
  }

  /**
   * Get graph head table name
   *
   * @return graph head table name
   */
  public String getGraphHeadTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.GRAPH);
  }

  @Override
  public String toString() {
    return Stream.of(GradoopAccumuloProperty.values())
      .collect(Collectors.toMap(GradoopAccumuloProperty::getKey, it -> it.get(accumuloProperties)))
      .toString();
  }

}
