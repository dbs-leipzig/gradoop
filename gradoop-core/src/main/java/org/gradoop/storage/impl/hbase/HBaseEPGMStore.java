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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.log4j.Logger;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.storage.api.EPGMStore;
import org.gradoop.storage.api.EdgeHandler;
import org.gradoop.storage.api.GraphHeadHandler;
import org.gradoop.storage.api.PersistentEdge;
import org.gradoop.storage.api.PersistentGraphHead;
import org.gradoop.storage.api.PersistentVertex;
import org.gradoop.storage.api.VertexHandler;
import org.gradoop.util.GConstants;
import org.gradoop.util.GradoopConfig;

import java.io.IOException;
import java.util.Iterator;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class HBaseEPGMStore<VD extends EPGMVertex, ED extends EPGMEdge, GD
  extends EPGMGraphHead> implements EPGMStore<VD, ED, GD> {
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
   * @param vertexTable HBase table to store vertex data
   * @param edgeTable   HBase table to store edge data
   * @param graphHeadTable  HBase table to store graph data
   * @param config          Gradoop Configuration
   */
  HBaseEPGMStore(final HTable vertexTable,
    final HTable edgeTable,
    final HTable graphHeadTable,
    final GradoopHBaseConfig<VD, ED, GD> config) {
    if (vertexTable == null) {
      throw new IllegalArgumentException("vertexTable must not be null");
    }
    if (edgeTable == null) {
      throw new IllegalArgumentException("edgeTable must not be null");
    }
    if (graphHeadTable == null) {
      throw new IllegalArgumentException("graphHeadTable must not be null");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }

    this.config = config;

    this.vertexTable = vertexTable;
    this.edgeTable = edgeTable;
    this.graphHeadTable = graphHeadTable;

    this.vertexTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.edgeTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.graphHeadTable
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
  public void writeGraphHead(final PersistentGraphHead graphHead) {
    LOG.info("Writing graph data: " + graphHead);
    try {
      GraphHeadHandler graphHeadHandler = config.getGraphHeadHandler();
      // graph id
      Put put = new Put(graphHeadHandler.getRowKey(graphHead.getId()));
      // write graph to Put
      put = graphHeadHandler.writeGraphHead(put, graphHead);
      // write to table
      graphHeadTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeVertex(final PersistentVertex<ED> vertexData) {
    LOG.info("Writing vertex data: " + vertexData);
    try {
      VertexHandler<VD, ED> vertexHandler = config.getVertexHandler();
      // vertex id
      Put put = new Put(vertexHandler.getRowKey(vertexData.getId()));
      // write vertex data to Put
      put = vertexHandler.writeVertex(put, vertexData);
      // write to table
      vertexTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeEdge(final PersistentEdge<VD> edgeData) {
    LOG.info("Writing edge data: " + edgeData);

    // write to table
    try {
      EdgeHandler<ED, VD> edgeHandler = config.getEdgeHandler();
      // edge id
      Put put = new Put(edgeHandler.getRowKey(edgeData.getId()));
      // write edge data to Put
      put = edgeHandler.writeEdge(put, edgeData);
      edgeTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GD readGraph(final GradoopId graphId) {
    GD graphData = null;
    try {
      GraphHeadHandler<GD> graphHeadHandler = config.getGraphHeadHandler();
      Result res = graphHeadTable.get(new Get(Writables.getBytes(graphId)));
      if (!res.isEmpty()) {
        graphData = graphHeadHandler.readGraphHead(res);
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
  public VD readVertex(final GradoopId vertexId) {
    VD vertexData = null;
    try {
      VertexHandler<VD, ED> vertexHandler = config.getVertexHandler();
      byte[] rowKey = vertexHandler.getRowKey(vertexId);
      Result res = vertexTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        vertexData = vertexHandler.readVertex(res);
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
  public ED readEdge(final GradoopId edgeId) {
    ED edgeData = null;
    try {
      EdgeHandler<ED, VD> edgeHandler = config.getEdgeHandler();
      byte[] rowKey = edgeHandler.getRowKey(edgeId);
      Result res = edgeTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        edgeData = edgeHandler.readEdge(res);
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
    return new GraphHeadIterator(graphHeadTable.getScanner(scan));
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
    return new VertexIterator(vertexTable.getScanner(scan));
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
    return new EdgeIterator(edgeTable.getScanner(scan));
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
  public void flush() {
    try {
      vertexTable.flushCommits();
      edgeTable.flushCommits();
      graphHeadTable.flushCommits();
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
      vertexTable.close();
      edgeTable.close();
      graphHeadTable.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * graph data.
   */
  public class GraphHeadIterator implements Iterator<GD> {
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
    public GraphHeadIterator(ResultScanner scanner) throws IOException {
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
        try {
          val = config.getGraphHeadHandler().readGraphHead(result);
        } catch (IOException e) {
          e.printStackTrace();
        }
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
  public class VertexIterator implements Iterator<VD> {
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
    public VertexIterator(ResultScanner scanner) throws IOException {
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
        try {
          val = config.getVertexHandler().readVertex(result);
        } catch (IOException e) {
          e.printStackTrace();
        }
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
  public class EdgeIterator implements Iterator<ED> {
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
    public EdgeIterator(ResultScanner scanner) throws IOException {
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
        try {
          val = config.getEdgeHandler().readEdge(result);
        } catch (IOException e) {
          e.printStackTrace();
        }
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
