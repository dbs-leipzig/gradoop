package org.gradoop.core.storage.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.core.model.Graph;
import org.gradoop.core.model.Vertex;
import org.gradoop.core.model.inmemory.SimpleGraph;
import org.gradoop.core.model.inmemory.SimpleVertex;
import org.gradoop.core.storage.GraphStore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public class HBaseGraphStore implements GraphStore {

  private static final Logger LOG = Logger.getLogger(HBaseGraphStore.class);

  static final String TABLE_VERTICES = "vertices";
  static final String TABLE_GRAPHS = "graphs";

  static final String CF_LABELS = "labels";
  static final String CF_PROPERTIES = "properties";
  static final String CF_VERTICES = "vertices";
  static final String CF_OUT_EDGES = "out_edges";
  static final String CF_IN_EDGES = "in_edges";
  static final String CF_GRAPHS = "graphs";

  private final HTable verticesTable;
  private final HTable graphsTable;

  private final VertexHandler verticesHandler;
  private final GraphHandler graphsHandler;

  public HBaseGraphStore(HTable graphsTable, HTable verticesTable,
                         VertexHandler verticesHandler,
                         GraphHandler graphsHandler) {
    this.graphsTable = graphsTable;
    this.verticesTable = verticesTable;
    this.verticesHandler = verticesHandler;
    this.graphsHandler = graphsHandler;
  }

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
      graphsTable.flushCommits();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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
      put = verticesHandler.writeOutgoingEdges(put, vertex);
      // incoming edges
      put = verticesHandler.writeIncomingEdges(put, vertex);
      // graphs
      put = verticesHandler.writeGraphs(put, vertex);

      verticesTable.put(put);
      verticesTable.flushCommits();
    } catch (RetriesExhaustedWithDetailsException e) {
      e.printStackTrace();
    } catch (InterruptedIOException e) {
      e.printStackTrace();
    }
  }

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
        g = new SimpleGraph(graphID, labels, properties, vertices);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return g;
  }

  @Override
  public Vertex readVertex(final Long vertexID) {
    Vertex v = null;
    try {
      byte[] rowKey = Bytes.toBytes(vertexID);
      Result res = verticesTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        Iterable<String> labels = verticesHandler.readLabels(res);
        Map<String, Object> properties = verticesHandler.readProperties(res);
        Map<String, Map<String, Object>> outEdges =
          verticesHandler.readOutgoingEdges(res);
        Map<String, Map<String, Object>> inEdges =
          verticesHandler.readIncomingEdges(res);
        Iterable<Long> graphs = verticesHandler.readGraphs(res);
        v = new SimpleVertex(vertexID, labels, properties, outEdges, inEdges,
          graphs);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return v;
  }

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
