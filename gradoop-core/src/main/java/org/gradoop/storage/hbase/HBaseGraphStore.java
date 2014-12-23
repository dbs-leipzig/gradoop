package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.log4j.Logger;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Default HBase graph store that handles reading and writing vertices and
 * graphs from and to HBase.
 */
public class HBaseGraphStore implements GraphStore {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(HBaseGraphStore.class);

  /**
   * Default value for clearing buffer on fail.
   */
  private static final boolean DEFAULT_CLEAR_BUFFER_ON_FAIL = true;
  /**
   * Default value for enabling auto flush in HBase.
   */
  private static final boolean DEFAULT_ENABLE_AUTO_FLUSH = true;

  /**
   * HBase table to use for storing vertices.
   */
  private final HTable verticesTable;
  /**
   * HBase table to use for storing graphs.
   */
  private final HTable graphsTable;

  /**
   * Handles the specific storing of vertices.
   */
  private final VertexHandler vertexHandler;
  /**
   * Handles the specific storing of graphs.
   */
  private final GraphHandler graphHandler;

  /**
   * Creates a HBaseGraphStore based on the given parameters. All parameters
   * are mandatory and must not be {@code null}.
   *
   * @param graphsTable   HBase table to store graphs
   * @param verticesTable HBase table to store vertices
   * @param vertexHandler handles reading/writing of vertices
   * @param graphHandler  handles reading/writing of graphs
   */
  HBaseGraphStore(final HTable graphsTable, final HTable verticesTable,
    final VertexHandler vertexHandler, final GraphHandler graphHandler) {
    if (graphsTable == null) {
      throw new IllegalArgumentException("graphsTable must not be null");
    }
    if (verticesTable == null) {
      throw new IllegalArgumentException("verticesTable must not be null");
    }
    if (vertexHandler == null) {
      throw new IllegalArgumentException("vertexHandler must not be null");
    }
    if (graphHandler == null) {
      throw new IllegalArgumentException("graphHandler must not be null");
    }
    this.graphsTable = graphsTable;
    this.verticesTable = verticesTable;
    this.vertexHandler = vertexHandler;
    this.graphHandler = graphHandler;

    this.verticesTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.graphsTable
      .setAutoFlush(DEFAULT_ENABLE_AUTO_FLUSH, DEFAULT_CLEAR_BUFFER_ON_FAIL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeGraph(final Graph graph) {
    LOG.info("writing: " + graph);
    try {
      // graph id
      Put put = new Put(graphHandler.getRowKey(graph.getID()));
      // write graph to Put
      put = graphHandler.writeGraph(put, graph);
      // write to table
      graphsTable.put(put);
    } catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeVertex(final Vertex vertex) {
    LOG.info("writing: " + vertex);
    try {
      // vertex id
      Put put = new Put(vertexHandler.getRowKey(vertex.getID()));
      // write vertex to Put
      put = vertexHandler.writeVertex(put, vertex);
      // write to table
      verticesTable.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph readGraph(final Long graphID) {
    Graph g = null;
    try {
      byte[] rowKey = graphHandler.getRowKey(graphID);
      Result res = graphsTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        g = graphHandler.readGraph(res);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return g;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(final Long vertexID) {
    Vertex v = null;
    try {
      byte[] rowKey = vertexHandler.getRowKey(vertexID);
      Result res = verticesTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        v = vertexHandler.readVertex(res);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return v;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.verticesTable.setAutoFlush(autoFlush, true);
    this.graphsTable.setAutoFlush(autoFlush, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flush() {
    try {
      this.verticesTable.flushCommits();
      this.graphsTable.flushCommits();
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
      graphsTable.close();
      verticesTable.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
