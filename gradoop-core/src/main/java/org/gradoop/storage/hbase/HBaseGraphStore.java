package org.gradoop.storage.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.model.Edge;
import org.gradoop.model.Graph;
import org.gradoop.model.Vertex;
import org.gradoop.storage.GraphStore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.List;

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
  public VertexHandler getVertexHandler() {
    return this.vertexHandler;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHandler getGraphHandler() {
    return this.graphHandler;
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


  @Override
  public Iterator<Graph> getGraphs(String graphsTableName) {
    GraphIterator graphIterator = null;

    try {
      Scan scan = new Scan();
      scan.setCaching(500);
      scan.setMaxVersions(1);

      ResultScanner scanner = graphsTable.getScanner(scan);
      graphIterator = new GraphIterator(scanner);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return graphIterator;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Vertex> getVertices(String tableName) throws
    InterruptedException, IOException, ClassNotFoundException {
    VertexIterator vertexIterator = null;

    try {
      Scan scan = new Scan();
      scan.setCaching(500);
      scan.setMaxVersions(1);

      ResultScanner scanner = verticesTable.getScanner(scan);
      vertexIterator = new VertexIterator(scanner);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return vertexIterator;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getRowCount(String tableName) throws IOException,
    ClassNotFoundException, InterruptedException {
    Job rcJob = RowCounter
      .createSubmittableJob(new Configuration(), new String[]{tableName});

    rcJob.waitForCompletion(true);
    return rcJob.getCounters().findCounter(GConstants.ROW_COUNTER_MAPRED_JOB,
      GConstants.ROW_COUNTER_PROPERTY).getValue();
  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * vertices
   */
  public class VertexIterator implements Iterator<Vertex> {
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
     * @param scanner HBase result scanner
     * @throws IOException
     */
    public VertexIterator(ResultScanner scanner) throws IOException {
      this.it = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      if (it.hasNext()) {
        result = it.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Vertex next() {
      if (result != null) {
        Vertex vertex = vertexHandler.readVertex(result);
        result = null;
        return vertex;
      } else {
        throw new NullPointerException();
      }
    }

    @Override
    public void remove() {
    }

  }

  /**
   * Iterator helper class for iterating over HBase result scanner containing
   * vertices
   */
  public class GraphIterator implements Iterator<Graph> {
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
     * @param scanner HBase result scanner
     * @throws IOException
     */
    public GraphIterator(ResultScanner scanner) throws IOException {
      this.it = scanner.iterator();
    }

    @Override
    public boolean hasNext() {
      if (it.hasNext()) {
        result = it.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Graph next() {
      if (result != null) {
        Graph graph = graphHandler.readGraph(result);
        result = null;
        return graph;
      } else {
        throw new NullPointerException();
      }
    }

    @Override
    public void remove() {
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Edge> getEdges() {
    List<Edge> eList = Lists.newArrayList();
    try {
      ResultScanner scanner = verticesTable.getScanner(new Scan());
      for (Result res : scanner) {
        if (!res.isEmpty()) {
          for (Edge edge : vertexHandler.readOutgoingEdges(res)) {
            eList.add(edge);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return eList;
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
