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
import org.gradoop.storage.impl.hbase.iterator.HBaseEdgeIterator;
import org.gradoop.storage.impl.hbase.iterator.HBaseGraphIterator;
import org.gradoop.storage.impl.hbase.iterator.HBaseVertexIterator;
import org.gradoop.storage.impl.hbase.predicate.filter.HBaseFilterUtils;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

  @Override
  public GradoopHBaseConfig getConfig() {
    return config;
  }

  @Override
  public String getVertexTableName() {
    return vertexTable.getName().getNameAsString();
  }

  @Override
  public String getEdgeTableName() {
    return edgeTable.getName().getNameAsString();
  }

  @Override
  public String getGraphHeadName() {
    return graphHeadTable.getName().getNameAsString();
  }

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

  @Override
  public GraphHead readGraph(@Nonnull final GradoopId graphId) throws IOException {
    GraphHead graphData = null;
    GraphHeadHandler graphHeadHandler = config.getGraphHeadHandler();
    List<Get> getList = new ArrayList<>();

    if (graphHeadHandler.isSpreadingByteUsed()) {
      for (byte[] rowKey : graphHeadHandler.getPossibleRowKeys(graphId)) {
        getList.add(new Get(rowKey));
      }
    } else {
      getList.add(new Get(graphId.toByteArray()));
    }
    final Result[] results = graphHeadTable.get(getList);
    for (Result res : results) {
      if (!res.isEmpty()) {
        graphData = graphHeadHandler.readGraphHead(res);
        break;
      }
    }
    return graphData;
  }

  @Override
  public Vertex readVertex(@Nonnull final GradoopId vertexId) throws IOException {
    Vertex vertexData = null;
    VertexHandler vertexHandler = config.getVertexHandler();
    List<Get> getList = new ArrayList<>();

    if (vertexHandler.isSpreadingByteUsed()) {
      for (byte[] rowKey : vertexHandler.getPossibleRowKeys(vertexId)) {
        getList.add(new Get(rowKey));
      }
    } else {
      getList.add(new Get(vertexHandler.getRowKey(vertexId)));
    }

    final Result[] results = vertexTable.get(getList);
    for (Result res : results) {
      if (!res.isEmpty()) {
        vertexData = vertexHandler.readVertex(res);
        break;
      }
    }

    return vertexData;
  }

  @Override
  public Edge readEdge(@Nonnull final GradoopId edgeId) throws IOException {
    Edge edgeData = null;
    EdgeHandler edgeHandler = config.getEdgeHandler();
    List<Get> getList = new ArrayList<>();

    if (edgeHandler.isSpreadingByteUsed()) {
      for (byte[] rowKey : edgeHandler.getPossibleRowKeys(edgeId)) {
        getList.add(new Get(rowKey));
      }
    } else {
      getList.add(new Get(edgeHandler.getRowKey(edgeId)));
    }

    final Result[] results = edgeTable.get(getList);
    for (Result res : results) {
      if (!res.isEmpty()) {
        edgeData = edgeHandler.readEdge(res);
        break;
      }
    }

    return edgeData;
  }

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
      attachFilter(query, scan, config.getGraphHeadHandler().isSpreadingByteUsed());
    }

    return new HBaseGraphIterator(graphHeadTable.getScanner(scan), config.getGraphHeadHandler());
  }

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
      attachFilter(query, scan, config.getVertexHandler().isSpreadingByteUsed());
    }

    return new HBaseVertexIterator(vertexTable.getScanner(scan), config.getVertexHandler());
  }

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
      attachFilter(query, scan, config.getEdgeHandler().isSpreadingByteUsed());
    }

    return new HBaseEdgeIterator(edgeTable.getScanner(scan), config.getEdgeHandler());
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  @Override
  public void flush() throws IOException {
    admin.flush(vertexTable.getName());
    admin.flush(edgeTable.getName());
    admin.flush(graphHeadTable.getName());
  }

  @Override
  public void close() throws IOException {
    vertexTable.close();
    edgeTable.close();
    graphHeadTable.close();
  }

  /**
   * First disable, then drop all three tables.
   *
   * @throws IOException on error
   */
  public void dropTables() throws IOException {
    admin.disableTable(vertexTable.getName());
    admin.disableTable(edgeTable.getName());
    admin.disableTable(graphHeadTable.getName());

    admin.deleteTable(vertexTable.getName());
    admin.deleteTable(edgeTable.getName());
    admin.deleteTable(graphHeadTable.getName());
  }

  /**
   * First disable, then truncate all tables handled by this store instance, i.e. delete all rows.
   *
   * @throws IOException when truncating any table fails.
   */
  public void truncateTables() throws IOException {
    admin.disableTable(graphHeadTable.getName());
    admin.disableTable(vertexTable.getName());
    admin.disableTable(edgeTable.getName());

    admin.truncateTable(getConfig().getGraphTableName(), true);
    admin.truncateTable(getConfig().getVertexTableName(), true);
    admin.truncateTable(getConfig().getEdgeTableName(), true);
  }

  /**
   * Attach a HBase filter represented by the given query to the given scan instance.
   *
   * @param query the query that represents a filter
   * @param scan the HBase scan instance on which the filter will be applied
   * @param isSpreadingByteUsed indicates whether a spreading byte is used as row key prefix or not
   * @param <T> the type of the EPGM element
   */
  private <T extends EPGMElement> void attachFilter(
    @Nonnull ElementQuery<HBaseElementFilter<T>> query,
    @Nonnull Scan scan,
    boolean isSpreadingByteUsed) {

    FilterList conjunctFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (query.getQueryRanges() != null && !query.getQueryRanges().isEmpty()) {
      conjunctFilters.addFilter(HBaseFilterUtils.getIdFilter(query.getQueryRanges(),
        isSpreadingByteUsed));
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
