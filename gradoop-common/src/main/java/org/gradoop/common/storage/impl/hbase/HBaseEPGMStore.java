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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.common.storage.impl.hbase;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Writables;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.EPGMStore;
import org.gradoop.common.storage.api.EdgeHandler;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentEdge;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentVertex;
import org.gradoop.common.util.GConstants;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.api.VertexHandler;

import java.io.IOException;
import java.util.Iterator;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class HBaseEPGMStore
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements EPGMStore<G, V, E> {
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
  private final GradoopHBaseConfig<G, V, E> config;

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
  HBaseEPGMStore(final HTable graphHeadTable,
    final HTable vertexTable,
    final HTable edgeTable,
    final GradoopHBaseConfig<G, V, E> config) {
    this.graphHeadTable = Preconditions.checkNotNull(graphHeadTable);
    this.vertexTable = Preconditions.checkNotNull(vertexTable);
    this.edgeTable = Preconditions.checkNotNull(edgeTable);
    this.config = Preconditions.checkNotNull(config);

    this.graphHeadTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.vertexTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.edgeTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopHBaseConfig<G, V, E> getConfig() {
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
  public void writeVertex(final PersistentVertex<E> vertexData) {
    try {
      VertexHandler<V, E> vertexHandler = config.getVertexHandler();
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
  public void writeEdge(final PersistentEdge<V> edgeData) {
    // write to table
    try {
      EdgeHandler<E, V> edgeHandler = config.getEdgeHandler();
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
  public G readGraph(final GradoopId graphId) {
    G graphData = null;
    try {
      GraphHeadHandler<G> graphHeadHandler = config.getGraphHeadHandler();
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
  public V readVertex(final GradoopId vertexId) {
    V vertexData = null;
    try {
      VertexHandler<V, E> vertexHandler = config.getVertexHandler();
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
  public E readEdge(final GradoopId edgeId) {
    E edgeData = null;
    try {
      EdgeHandler<E, V> edgeHandler = config.getEdgeHandler();
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
  public Iterator<G> getGraphSpace() throws IOException {
    return getGraphSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<G> getGraphSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new GraphHeadIterator(graphHeadTable.getScanner(scan));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<V> getVertexSpace() throws IOException {
    return getVertexSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<V> getVertexSpace(int cacheSize) throws IOException {
    Scan scan = new Scan();
    scan.setCaching(cacheSize);
    scan.setMaxVersions(1);
    return new VertexIterator(vertexTable.getScanner(scan));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<E> getEdgeSpace() throws IOException {
    return getEdgeSpace(GConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<E> getEdgeSpace(int cacheSize) throws IOException {
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
  public class GraphHeadIterator implements Iterator<G> {
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
    public G next() {
      return config.getGraphHeadHandler().readGraphHead(result);
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
  public class VertexIterator implements Iterator<V> {
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
      boolean hasNext = false;
      if (it.hasNext()) {
        result = it.next();
        hasNext = result != null;
      }
      return hasNext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V next() {
      return config.getVertexHandler().readVertex(result);
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
  public class EdgeIterator implements Iterator<E> {
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
      boolean hasNext = false;
      if (it.hasNext()) {
        result = it.next();
        hasNext = result != null;
      }
      return hasNext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public E next() {
      return config.getEdgeHandler().readEdge(result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
    }
  }
}
