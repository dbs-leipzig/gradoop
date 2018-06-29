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
package org.gradoop.common.storage.impl.hbase;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.EPGMConfigProvider;
import org.gradoop.common.storage.api.EPGMGraphInput;
import org.gradoop.common.storage.api.EPGMGraphOutput;
import org.gradoop.common.storage.api.EPGMGraphPredictableOutput;
import org.gradoop.common.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.common.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.api.PersistentEdge;
import org.gradoop.common.storage.impl.hbase.api.PersistentGraphHead;
import org.gradoop.common.storage.impl.hbase.api.PersistentVertex;
import org.gradoop.common.storage.impl.hbase.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.iterator.HBaseEdgeIterator;
import org.gradoop.common.storage.impl.hbase.iterator.HBaseGraphIterator;
import org.gradoop.common.storage.impl.hbase.iterator.HBaseVertexIterator;
import org.gradoop.common.storage.iterator.ClosableIterator;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 *
 * @see EPGMGraphPredictableOutput
 */
public class HBaseEPGMStore implements
  EPGMConfigProvider<GradoopHBaseConfig>,
  EPGMGraphInput<PersistentGraphHead, PersistentVertex<Edge>, PersistentEdge<Vertex>>,
  EPGMGraphOutput {
  //TODO make HBaseEPGMStore implement EPGMGraphPredictableOutput

  /**
   * Default value for clearing buffer on fail.
   */
  private static final boolean DEFAULT_CLEAR_BUFFER_ON_FAIL = true;
  /**
   * Default value for enabling auto flush in HBase.
   */
  private static final boolean DEFAULT_ENABLE_AUTO_FLUSH = true;

  /**
   * Gradoop configuration.
   */
  private final GradoopHBaseConfig config;

  /**
   * HBase table for storing graphs.
   */
  private final HTable graphHeadTable;
  /**
   * HBase table for storing vertex data.
   */
  private final HTable vertexTable;
  /**
   * HBase table for storing edge data.
   */
  private final HTable edgeTable;

  /**
   * Creates a HBaseEPGMStore based on the given parameters. All parameters
   * are mandatory and must not be {@code null}.
   *
   * @param graphHeadTable  HBase table to store graph data
   * @param vertexTable     HBase table to store vertex data
   * @param edgeTable       HBase table to store edge data
   * @param config          Gradoop Configuration
   */
  public HBaseEPGMStore(
    final HTable graphHeadTable,
    final HTable vertexTable,
    final HTable edgeTable,
    final GradoopHBaseConfig config
  ) {
    this.graphHeadTable = Preconditions.checkNotNull(graphHeadTable);
    this.vertexTable = Preconditions.checkNotNull(vertexTable);
    this.edgeTable = Preconditions.checkNotNull(edgeTable);
    this.config = Preconditions.checkNotNull(config);

    this.graphHeadTable.setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.vertexTable.setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.edgeTable.setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopHBaseConfig getConfig() {
    return config;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getVertexTableName() {
    return vertexTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getEdgeTableName() {
    return edgeTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getGraphHeadName() {
    return graphHeadTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeGraphHead(@Nonnull final PersistentGraphHead graphHead) throws IOException {
    GraphHeadHandler graphHeadHandler = config.getGraphHeadHandler();
    // graph id
    Put put = new Put(graphHeadHandler.getRowKey(graphHead.getId()));
    // write graph to Put
    put = graphHeadHandler.writeGraphHead(put, graphHead);
    // write to table
    graphHeadTable.put(put);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeVertex(@Nonnull final PersistentVertex<Edge> vertexData) throws IOException {
    VertexHandler<Vertex, Edge> vertexHandler = config.getVertexHandler();
    // vertex id
    Put put = new Put(vertexHandler.getRowKey(vertexData.getId()));
    // write vertex data to Put
    put = vertexHandler.writeVertex(put, vertexData);
    // write to table
    vertexTable.put(put);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeEdge(@Nonnull final PersistentEdge<Vertex> edgeData) throws IOException {
    // write to table
    EdgeHandler<Edge, Vertex> edgeHandler = config.getEdgeHandler();
    // edge id
    Put put = new Put(edgeHandler.getRowKey(edgeData.getId()));
    // write edge data to Put
    put = edgeHandler.writeEdge(put, edgeData);
    edgeTable.put(put);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead readGraph(@Nonnull final GradoopId graphId) throws IOException {
    GraphHead graphData = null;
    GraphHeadHandler<GraphHead> graphHeadHandler = config.getGraphHeadHandler();
    Result res = graphHeadTable.get(new Get(graphId.toByteArray()));
    if (!res.isEmpty()) {
      graphData = graphHeadHandler.readGraphHead(res);
    }
    return graphData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(@Nonnull final GradoopId vertexId) throws IOException {
    Vertex vertexData = null;
    VertexHandler<Vertex, Edge> vertexHandler = config.getVertexHandler();
    byte[] rowKey = vertexHandler.getRowKey(vertexId);
    Result res = vertexTable.get(new Get(rowKey));
    if (!res.isEmpty()) {
      vertexData = vertexHandler.readVertex(res);
    }
    return vertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge readEdge(@Nonnull final GradoopId edgeId) throws IOException {
    Edge edgeData = null;
    EdgeHandler<Edge, Vertex> edgeHandler = config.getEdgeHandler();
    byte[] rowKey = edgeHandler.getRowKey(edgeId);
    Result res = edgeTable.get(new Get(rowKey));
    if (!res.isEmpty()) {
      edgeData = edgeHandler.readEdge(res);
    }
    return edgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public ClosableIterator<GraphHead> getGraphSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new HBaseGraphIterator<>(graphHeadTable.getScanner(scan), config.getGraphHeadHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public ClosableIterator<Vertex> getVertexSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new HBaseVertexIterator<>(vertexTable.getScanner(scan), config.getVertexHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Nonnull
  public ClosableIterator<Edge> getEdgeSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new HBaseEdgeIterator<>(edgeTable.getScanner(scan), config.getEdgeHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    vertexTable.setAutoFlush(autoFlush, true);
    edgeTable.setAutoFlush(autoFlush, true);
    graphHeadTable.setAutoFlush(autoFlush, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    vertexTable.flushCommits();
    edgeTable.flushCommits();
    graphHeadTable.flushCommits();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    vertexTable.close();
    edgeTable.close();
    graphHeadTable.close();
  }

}
