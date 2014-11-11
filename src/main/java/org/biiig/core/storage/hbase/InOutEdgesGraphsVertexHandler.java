package org.biiig.core.storage.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.biiig.core.model.Vertex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by s1ck on 11/8/14.
 */
public class InOutEdgesGraphsVertexHandler extends BasicHandler
    implements VertexHandler {
  private static Logger LOG = Logger.getLogger(InOutEdgesGraphsVertexHandler.class);

  private static final byte[] CF_OUT_EDGES_BYTES = Bytes.toBytes(HBaseGraphStore.CF_OUT_EDGES);
  private static final byte[] CF_IN_EDGES_BYTES = Bytes.toBytes(HBaseGraphStore.CF_IN_EDGES);
  private static final byte[] CF_GRAPHS_BYTES = Bytes.toBytes(HBaseGraphStore.CF_GRAPHS);

  @Override public void createVerticesTable(HBaseAdmin admin,
      HTableDescriptor tableDescriptor) throws IOException {
    LOG.info("creating table " + tableDescriptor.getNameAsString());
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_LABELS));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_PROPERTIES));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_OUT_EDGES));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_IN_EDGES));
    tableDescriptor.addFamily(new HColumnDescriptor(HBaseGraphStore.CF_GRAPHS));
    admin.createTable(tableDescriptor);
  }

  @Override public Put writeOutgoingEdges(Put put, Vertex vertex) {
    return writeEdges(put, CF_OUT_EDGES_BYTES, vertex.getOutgoingEdges());
  }

  @Override public Put writeIncomingEdges(Put put, Vertex vertex) {
    return writeEdges(put, CF_IN_EDGES_BYTES, vertex.getIncomingEdges());
  }

  @Override public Put writeGraphs(Put put, Vertex vertex) {
    for (Long graphID : vertex.getGraphs()) {
      put.add(CF_GRAPHS_BYTES, Bytes.toBytes(graphID), null);
    }
    return put;
  }

  @Override public Map<String, Map<String, Object>> readOutgoingEdges(Result res) {
    return readEdges(res, CF_OUT_EDGES_BYTES);
  }

  @Override public Map<String, Map<String, Object>> readIncomingEdges(Result res) {
    return readEdges(res, CF_IN_EDGES_BYTES);
  }

  @Override public Iterable<Long> readGraphs(Result res) {
    return getColumnKeysFromFamiliy(res, CF_GRAPHS_BYTES);
  }

  private Put writeEdges(Put put, byte[] columnFamily, Map<String, Map<String, Object>> edges) {
    for (Map.Entry<String, Map<String, Object>> edge : edges.entrySet()) {
      put.add(columnFamily, Bytes.toBytes(edge.getKey()), null);
      // TODO: write properties
    }
    return put;
  }

  private Map<String, Map<String, Object>> readEdges(Result res, byte[] columnFamily) {
    Map<String, Map<String, Object>> edges = new HashMap<>();
    for (Map.Entry<byte[], byte[]> edgeColumn : res.getFamilyMap(columnFamily).entrySet()) {
      String edgeKey = Bytes.toString(edgeColumn.getKey());
      Map<String, Object> edgeProperties = new HashMap<>();
      // TODO: read properties
      edges.put(edgeKey, edgeProperties);
    }
    return edges;
  }
}
