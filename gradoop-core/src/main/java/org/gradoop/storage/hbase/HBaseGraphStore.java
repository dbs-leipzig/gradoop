package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.model.Edge;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryGraph;
import org.gradoop.model.inmemory.MemoryVertex;
import org.gradoop.storage.GraphStore;

import java.io.IOException;
import java.util.Map;

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
  private final VertexHandler verticesHandler;
  /**
   * Handles the specific storing of graphs.
   */
  private final GraphHandler graphsHandler;

  private static final boolean DEFAULT_AUTO_FLUSH = true;

  private static final boolean DEFAULT_CLEAR_BUFFER_ON_FAIL = true;

  /**
   * Creates a HBaseGraphStore based on the given parameters. All parameters
   * are mandatory and must not be {@code null}.
   *
   * @param graphsTable     HBase table to store graphs
   * @param verticesTable   HBase table to store vertices
   * @param verticesHandler handles reading/writing of vertices
   * @param graphsHandler   handles reading/writing of graphs
   */
  HBaseGraphStore(final HTable graphsTable, final HTable verticesTable,
                  final VertexHandler verticesHandler,
                  final GraphHandler graphsHandler) {
    if (graphsTable == null) {
      throw new IllegalArgumentException("graphsTable must not be null");
    }
    if (verticesTable == null) {
      throw new IllegalArgumentException("verticesTable must not be null");
    }
    if (verticesHandler == null) {
      throw new IllegalArgumentException("verticesHandler must not be null");
    }
    if (graphsHandler == null) {
      throw new IllegalArgumentException("graphsHandler must not be null");
    }
    this.graphsTable = graphsTable;
    this.verticesTable = verticesTable;
    this.verticesHandler = verticesHandler;
    this.graphsHandler = graphsHandler;

    this.verticesTable.setAutoFlush(DEFAULT_AUTO_FLUSH,
      DEFAULT_CLEAR_BUFFER_ON_FAIL);
    this.graphsTable.setAutoFlush(DEFAULT_AUTO_FLUSH,
      DEFAULT_CLEAR_BUFFER_ON_FAIL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeGraph(final Graph graph) {
    LOG.info("writing: " + graph);
    try {
      // graph id
      Put put = new Put(Bytes.toBytes(graph.getID()));
      // graph labels
      put = graphsHandler.writeLabels(put, graph);
      // graph properties
      put = graphsHandler.writeProperties(put, graph);
      // graph vertices
      put = graphsHandler.writeVertices(put, graph);

      graphsTable.put(put);
    } catch (Exception e) {
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
      Put put = new Put(Bytes.toBytes(vertex.getID()));
      // vertex labels
      put = verticesHandler.writeLabels(put, vertex);
      // vertex properties
      put = verticesHandler.writeProperties(put, vertex);
      // outgoing edges
      put = verticesHandler.writeOutgoingEdges(put, vertex.getOutgoingEdges());
      // incoming edges
      put = verticesHandler.writeIncomingEdges(put, vertex.getIncomingEdges());
      // graphs
      put = verticesHandler.writeGraphs(put, vertex);

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
      byte[] rowKey = Bytes.toBytes(graphID);
      Result res = graphsTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        Iterable<String> labels = graphsHandler.readLabels(res);
        Map<String, Object> properties = graphsHandler.readProperties(res);
        Iterable<Long> vertices = graphsHandler.readVertices(res);
        g = new MemoryGraph(graphID, labels, properties, vertices);
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
      byte[] rowKey = Bytes.toBytes(vertexID);
      Result res = verticesTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        Iterable<String> labels = verticesHandler.readLabels(res);
        Map<String, Object> properties = verticesHandler.readProperties(res);
        Iterable<Edge> outEdges = verticesHandler.readOutgoingEdges(res);
        Iterable<Edge> inEdges = verticesHandler.readIncomingEdges(res);
        Iterable<Long> graphs = verticesHandler.readGraphs(res);
        v = new MemoryVertex(vertexID, labels, properties, outEdges, inEdges,
          graphs);
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
