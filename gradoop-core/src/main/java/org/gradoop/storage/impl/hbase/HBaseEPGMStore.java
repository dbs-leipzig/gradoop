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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.storage.impl.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.EdgeDataHandler;
import org.gradoop.storage.api.GraphDataHandler;
import org.gradoop.storage.api.PersistentEdgeData;
import org.gradoop.storage.api.PersistentGraphData;
import org.gradoop.storage.api.PersistentVertexData;
import org.gradoop.storage.api.VertexDataHandler;
import org.gradoop.util.GConstants;
import org.gradoop.util.GradoopConfig;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class HBaseEPGMStore<VD extends VertexData, ED extends EdgeData, GD
  extends GraphData> implements
  EPGMStore<VD, ED, GD> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(HBaseEPGMStore.class);
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
  private final GradoopHBaseConfig<VD, ED, GD> config;

  /**
   * HBase table for storing graphs.
   */
  private final HTable graphDataTable;
  /**
   * HBase table for storing vertex data.
   */
  private final HTable vertexDataTable;
  /**
   * HBase table for storing edge data.
   */
  private final HTable edgeDataTable;

  /**
   * Creates a HBaseEPGMStore based on the given parameters. All parameters
   * are mandatory and must not be {@code null}.
   *
   * @param vertexDataTable HBase table to store vertex data
   * @param edgeDataTable   HBase table to store edge data
   * @param graphDataTable  HBase table to store graph data
   * @param config          Gradoop Configuration
   */
  HBaseEPGMStore(final HTable vertexDataTable,
    final HTable edgeDataTable,
    final HTable graphDataTable,
    final GradoopHBaseConfig<VD, ED, GD> config) {
    if (vertexDataTable == null) {
      throw new IllegalArgumentException("vertexDataTable must not be null");
    }
    if (edgeDataTable == null) {
      throw new IllegalArgumentException("edgeDataTable must not be null");
    }
    if (graphDataTable == null) {
      throw new IllegalArgumentException("graphDataTable must not be null");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }

    this.config = config;

    this.vertexDataTable = vertexDataTable;
    this.edgeDataTable = edgeDataTable;
    this.graphDataTable = graphDataTable;

    this.vertexDataTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.edgeDataTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.graphDataTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopConfig<VD, ED, GD> getConfig() {
    return config;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getVertexDataTableName() {
    return vertexDataTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getEdgeDataTableName() {
    return edgeDataTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getGraphDataTableName() {
    return graphDataTable.getName().getNameAsString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeGraphData(final PersistentGraphData graphData) {
    LOG.info("Writing graph data: " + graphData);
    try {
      GraphDataHandler graphHeadHandler = config.getGraphHeadHandler();
      // graph id
      Put put = new Put(graphHeadHandler.getRowKey(graphData.getId()));
      // write graph to Put
      put = graphHeadHandler.writeGraphData(put, graphData);
      // write to table
      graphDataTable.put(put);
    } catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeVertexData(final PersistentVertexData<ED> vertexData) {
    LOG.info("Writing vertex data: " + vertexData);
    try {
      VertexDataHandler<VD, ED> vertexHandler = config.getVertexHandler();
      // vertex id
      Put put = new Put(vertexHandler.getRowKey(vertexData.getId()));
      // write vertex data to Put
      put = vertexHandler.writeVertexData(put, vertexData);
      // write to table
      vertexDataTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeEdgeData(final PersistentEdgeData<VD> edgeData) {
    LOG.info("Writing edge data: " + edgeData);
    EdgeDataHandler<ED, VD> edgeHandler = config.getEdgeHandler();
    // edge id
    Put put = new Put(edgeHandler.getRowKey(edgeData.getId()));
    // write edge data to Put
    put = edgeHandler.writeEdgeData(put, edgeData);
    // write to table
    try {
      edgeDataTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GD readGraphData(final Long graphId) {
    GD graphData = null;
    try {
      GraphDataHandler<GD> graphDataHandler = config.getGraphHeadHandler();
      byte[] rowKey = graphDataHandler.getRowKey(graphId);
      Result res = graphDataTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        graphData = graphDataHandler.readGraphData(res);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return graphData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VD readVertexData(final Long vertexId) {
    VD vertexData = null;
    try {
      VertexDataHandler<VD, ED> vertexHandler = config.getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertexId);
      Result res = vertexDataTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        vertexData = vertexHandler.readVertexData(res);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return vertexData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ED readEdgeData(final Long edgeId) {
    ED edgeData = null;
    try {
      EdgeDataHandler<ED, VD> edgeHandler = config.getEdgeHandler();
      byte[] rowKey = edgeHandler.getRowKey(edgeId);
      Result res = edgeDataTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        edgeData = edgeHandler.readEdgeData(res);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return edgeData;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GD> getGraphSpace() throws IOException {
    return getGraphSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GD> getGraphSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new GraphDataIterator(graphDataTable.getScanner(scan));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<VD> getVertexSpace() throws IOException {
    return getVertexSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<VD> getVertexSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new VertexDataIterator(vertexDataTable.getScanner(scan));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<ED> getEdgeSpace() throws IOException {
    return getEdgeSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<ED> getEdgeSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new EdgeDataIterator(edgeDataTable.getScanner(scan));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    vertexDataTable.setAutoFlush(autoFlush, true);
    edgeDataTable.setAutoFlush(autoFlush, true);
    graphDataTable.setAutoFlush(autoFlush, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    try {
      vertexDataTable.flushCommits();
      edgeDataTable.flushCommits();
      graphDataTable.flushCommits();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    try {
      vertexDataTable.close();
      edgeDataTable.close();
      graphDataTable.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * graph data.
   */
  public class GraphDataIterator implements Iterator<GD> {
    /**
     * HBase result
     */
    private Result result = null;
    /**
     * Result iterator
     */
    private final Iterator<Result> it;

    /**
     * Constructor
     *
     * @param scanner HBase result scanner
     * @throws IOException
     */
    public GraphDataIterator(ResultScanner scanner) throws IOException {
      this.it = scanner.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
      if (it.hasNext()) {
        result = it.next();
        return true;
      } else {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GD next() {
      GD val = null;
      if (result != null) {
        val = config.getGraphHeadHandler().readGraphData(result);
        result = null;
      }
      return val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
    }
  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * vertex data.
   */
  public class VertexDataIterator implements Iterator<VD> {
    /**
     * HBase result
     */
    private Result result = null;
    /**
     * Result iterator
     */
    private Iterator<Result> it;

    /**
     * Constructor
     *
     * @param scanner HBase result scanner
     * @throws IOException
     */
    public VertexDataIterator(ResultScanner scanner) throws IOException {
      this.it = scanner.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
      if (it.hasNext()) {
        result = it.next();
        return true;
      } else {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VD next() {
      VD val = null;
      if (result != null) {
        val = config.getVertexHandler().readVertexData(result);
        result = null;
      }
      return val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
    }
  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * edge data.
   */
  public class EdgeDataIterator implements Iterator<ED> {
    /**
     * HBase result
     */
    private Result result = null;
    /**
     * Result iterator
     */
    private Iterator<Result> it;

    /**
     * Constructor
     *
     * @param scanner HBase result scanner
     * @throws IOException
     */
    public EdgeDataIterator(ResultScanner scanner) throws IOException {
      this.it = scanner.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
      if (it.hasNext()) {
        result = it.next();
        return true;
      } else {
        return false;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ED next() {
      ED val = null;
      if (result != null) {
        val = config.getEdgeHandler().readEdgeData(result);
        result = null;
      }
      return val;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
    }
  }
}
