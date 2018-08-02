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
package org.gradoop.storage.impl.hbase;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.api.EPGMConfigProvider;
import org.gradoop.storage.common.api.EPGMGraphInput;
import org.gradoop.storage.common.api.EPGMGraphPredictableOutput;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.storage.impl.hbase.api.VertexHandler;
import org.gradoop.storage.impl.hbase.predicate.filter.HBaseFilterUtils;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.impl.hbase.iterator.HBaseEdgeIterator;
import org.gradoop.storage.impl.hbase.iterator.HBaseGraphIterator;
import org.gradoop.storage.impl.hbase.iterator.HBaseVertexIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 *
 * @see EPGMGraphPredictableOutput
 */
public class HBaseEPGMStore implements
  EPGMConfigProvider<GradoopHBaseConfig>,
  EPGMGraphInput,
  EPGMGraphPredictableOutput<
    HBaseElementFilter<GraphHead>,
    HBaseElementFilter<Vertex>,
    HBaseElementFilter<Edge>> {

  /**
   * Gradoop configuration.
   */
  private final GradoopHBaseConfig config;
  /**
   * HBase table for storing graphs.
   */
  private final Table graphHeadTable;
  /**
   * HBase table for storing vertex data.
   */
  private final Table vertexTable;
  /**
   * HBase table for storing edge data.
   */
  private final Table edgeTable;
  /**
   * HBase admin instance
   */
  private final Admin admin;
  /**
   * Auto flush flag, default false
   */
  private volatile boolean autoFlush;

  /**
   * Creates a HBaseEPGMStore based on the given parameters. All parameters
   * are mandatory and must not be {@code null}.
   *
   * @param graphHeadTable HBase table to store graph data
   * @param vertexTable HBase table to store vertex data
   * @param edgeTable HBase table to store edge data
   * @param config Gradoop Configuration
   * @param admin HBase admin instance
   */
  public HBaseEPGMStore(
    final Table graphHeadTable,
    final Table vertexTable,
    final Table edgeTable,
    final GradoopHBaseConfig config,
    final Admin admin
  ) {
    this.graphHeadTable = Preconditions.checkNotNull(graphHeadTable);
    this.vertexTable = Preconditions.checkNotNull(vertexTable);
    this.edgeTable = Preconditions.checkNotNull(edgeTable);
    this.config = Preconditions.checkNotNull(config);
    this.admin = Preconditions.checkNotNull(admin);
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
  public void writeGraphHead(@Nonnull final EPGMGraphHead graphHead) throws IOException {
    GraphHeadHandler graphHeadHandler = config.getGraphHeadHandler();
    // graph id
    Put put = new Put(graphHeadHandler.getRowKey(graphHead.getId()));
    // write graph to Put
    put = graphHeadHandler.writeGraphHead(put, graphHead);
    // write to table
    graphHeadTable.put(put);
    if (autoFlush) {
      admin.flush(graphHeadTable.getName());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeVertex(@Nonnull final EPGMVertex vertexData) throws IOException {
    VertexHandler vertexHandler = config.getVertexHandler();
    // vertex id
    Put put = new Put(vertexHandler.getRowKey(vertexData.getId()));
    // write vertex data to Put
    put = vertexHandler.writeVertex(put, vertexData);
    // write to table
    vertexTable.put(put);
    if (autoFlush) {
      admin.flush(vertexTable.getName());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeEdge(@Nonnull final EPGMEdge edgeData) throws IOException {
    // write to table
    EdgeHandler edgeHandler = config.getEdgeHandler();
    // edge id
    Put put = new Put(edgeHandler.getRowKey(edgeData.getId()));
    // write edge data to Put
    put = edgeHandler.writeEdge(put, edgeData);
    edgeTable.put(put);
    if (autoFlush) {
      admin.flush(edgeTable.getName());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead readGraph(@Nonnull final GradoopId graphId) throws IOException {
    GraphHead graphData = null;
    GraphHeadHandler graphHeadHandler = config.getGraphHeadHandler();
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
    VertexHandler vertexHandler = config.getVertexHandler();
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
    EdgeHandler edgeHandler = config.getEdgeHandler();
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
  @Nonnull
  @Override
  public ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<HBaseElementFilter<GraphHead>> query,
    int cacheSize
  ) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);

    if (query != null) {
      attachFilter(query, scan);
    }

    return new HBaseGraphIterator(graphHeadTable.getScanner(scan), config.getGraphHeadHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<HBaseElementFilter<Vertex>> query,
    int cacheSize
  ) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);

    if (query != null) {
      attachFilter(query, scan);
    }

    return new HBaseVertexIterator(vertexTable.getScanner(scan), config.getVertexHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  public ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<HBaseElementFilter<Edge>> query,
    int cacheSize
  ) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);

    if (query != null) {
      attachFilter(query, scan);
    }

    return new HBaseEdgeIterator(edgeTable.getScanner(scan), config.getEdgeHandler());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() throws IOException {
    admin.flush(vertexTable.getName());
    admin.flush(edgeTable.getName());
    admin.flush(graphHeadTable.getName());
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

  /**
   * Attach a HBase filter represented by the given query to the given scan instance.
   *
   * @param query the query that represents a filter
   * @param scan the HBase scan instance on which the filter will be applied
   * @param <T> the type of the EPGM element
   */
  private <T extends EPGMElement> void attachFilter(
    @Nonnull ElementQuery<HBaseElementFilter<T>> query,
    @Nonnull Scan scan
  ) {
    FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (query.getQueryRanges() != null && !query.getQueryRanges().isEmpty()) {
      conjunctFilters.addFilter(HBaseFilterUtils.getIdFilter(query.getQueryRanges()));
    }

    if (query.getFilterPredicate() != null) {
      conjunctFilters.addFilter(query.getFilterPredicate().toHBaseFilter(false));
    }

    // if there are filters inside the root list, add it to the Scan object
    if (!conjunctFilters.getFilters().isEmpty()) {
      scan.setFilter(conjunctFilters);
    }
  }

}
