/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.storage.common.config.GradoopStoreConfig;
import org.gradoop.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.storage.impl.hbase.handler.HBaseEdgeHandler;
import org.gradoop.storage.impl.hbase.handler.HBaseGraphHeadHandler;
import org.gradoop.storage.impl.hbase.handler.HBaseVertexHandler;
import org.gradoop.storage.utils.RegionSplitter;
import org.gradoop.storage.utils.RowKeyDistributor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration class for using HBase with Gradoop.
 */
public class GradoopHBaseConfig implements GradoopStoreConfig {

  /**
   * Definition for serialize version control
   */
  private static final int serialVersionUID = 23;

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
  private final GraphHeadHandler graphHeadHandler;

  /**
   * EPGMVertex handler.
   */
  private final VertexHandler vertexHandler;

  /**
   * EPGMEdge handler.
   */
  private final EdgeHandler edgeHandler;

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
    GraphHeadHandler graphHeadHandler,
    VertexHandler vertexHandler,
    EdgeHandler edgeHandler,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "EPGMVertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "EPGMEdge table name was null or empty");

    this.graphTableName = graphTableName;
    this.vertexTableName = vertexTableName;
    this.edgeTableName = edgeTableName;

    this.graphHeadHandler = checkNotNull(graphHeadHandler, "GraphHeadHandler was null");
    this.vertexHandler = checkNotNull(vertexHandler, "VertexHandler was null");
    this.edgeHandler = checkNotNull(edgeHandler, "EdgeHandler was null");
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(
    GradoopHBaseConfig config,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
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
  public static GradoopHBaseConfig getDefaultConfig() {
    GraphHeadHandler graphHeadHandler = new HBaseGraphHeadHandler(new GraphHeadFactory());
    VertexHandler vertexHandler = new HBaseVertexHandler(new VertexFactory());
    EdgeHandler edgeHandler = new HBaseEdgeHandler(new EdgeFactory());

    return new GradoopHBaseConfig(
      graphHeadHandler,
      vertexHandler,
      edgeHandler,
      HBaseConstants.DEFAULT_TABLE_GRAPHS,
      HBaseConstants.DEFAULT_TABLE_VERTICES,
      HBaseConstants.DEFAULT_TABLE_EDGES
    );
  }

  /**
   * Creates a Gradoop HBase configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   *
   * @return Gradoop HBase configuration
   */
  public static GradoopHBaseConfig createConfig(
    GradoopHBaseConfig gradoopConfig,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
    return new GradoopHBaseConfig(gradoopConfig, graphTableName, vertexTableName, edgeTableName);
  }

  /**
   * Get vertex table name
   *
   * @return vertex table name
   */
  public TableName getVertexTableName() {
    return TableName.valueOf(vertexTableName);
  }

  /**
   * Get edge table name
   *
   * @return edge table name
   */
  public TableName getEdgeTableName() {
    return TableName.valueOf(edgeTableName);
  }

  /**
   * Get graph table name
   *
   * @return graph table name
   */
  public TableName getGraphTableName() {
    return TableName.valueOf(graphTableName);
  }

  /**
   * Get graph head handler
   *
   * @return graph head handler
   */
  public GraphHeadHandler getGraphHeadHandler() {
    return graphHeadHandler;
  }

  /**
   * Get vertex handler
   *
   * @return vertex handler
   */
  public VertexHandler getVertexHandler() {
    return vertexHandler;
  }

  /**
   * Get edge handler
   *
   * @return edge handler
   */
  public EdgeHandler getEdgeHandler() {
    return edgeHandler;
  }

  /**
   * Enable/Disable the usage of pre-splitting regions at the moment of table creation.
   * If the HBase table size grows, it should be created with pre-split regions in order to avoid
   * region hotspots. If certain region servers are stressed by very intensive write/read
   * operations, HBase may drop that region server because the Zookeeper connection will timeout.
   *
   * Note that this flag has no effect if the tables already exist.
   *
   * @param numberOfRegions the number of regions used for splitting
   * @return this modified config
   */
  public GradoopHBaseConfig enablePreSplitRegions(final int numberOfRegions) {
    RegionSplitter.getInstance().setNumberOfRegions(numberOfRegions);
    this.vertexHandler.setPreSplitRegions(true);
    this.edgeHandler.setPreSplitRegions(true);
    this.graphHeadHandler.setPreSplitRegions(true);
    return this;
  }

  /**
   * Enable/Disable the usage of a spreading byte as prefix of each HBase row key. This affects
   * reading and writing from/to HBase.
   *
   * Records in HBase are sorted lexicographically by the row key. This allows fast access to
   * an individual record by its key and fast fetching of a range of data given start and stop keys.
   * But writing records with such naive keys will cause hotspotting because of how HBase writes
   * data to its Regions. With this option you can disable this hotspotting.
   *
   * @param bucketCount the number of spreading bytes to use
   * @return this modified config
   */
  public GradoopHBaseConfig useSpreadingByte(final int bucketCount) {
    RowKeyDistributor.getInstance().setBucketCount((byte) bucketCount);
    this.vertexHandler.setSpreadingByteUsage(true);
    this.edgeHandler.setSpreadingByteUsage(true);
    this.graphHeadHandler.setSpreadingByteUsage(true);
    return this;
  }
}
