package org.biiig.epg.store.impl;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.biiig.epg.model.Attributed;
import org.biiig.epg.model.Graph;
import org.biiig.epg.model.Vertex;
import org.biiig.epg.model.impl.SimpleGraph;
import org.biiig.epg.model.impl.SimpleVertex;
import org.biiig.epg.store.GraphStore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  private static final byte[] CF_LABELS_BYTES = Bytes.toBytes(CF_LABELS);
  private static final byte[] CF_PROPERTIES_BYTES = Bytes.toBytes(CF_PROPERTIES);
  private static final byte[] CF_VERTICES_BYTES = Bytes.toBytes(CF_VERTICES);
  private static final byte[] CF_OUT_EDGES_BYTES = Bytes.toBytes(CF_OUT_EDGES);
  private static final byte[] CF_IN_EDGES_BYTES = Bytes.toBytes(CF_IN_EDGES);

  private static final byte TYPE_BOOLEAN = 0x00;
  private static final byte TYPE_INTEGER = 0x01;
  private static final byte TYPE_LONG = 0x02;
  private static final byte TYPE_FLOAT = 0x03;
  private static final byte TYPE_DOUBLE = 0x04;
  private static final byte TYPE_STRING = 0x05;

  private final HTable verticesTable;

  private final HTable graphsTable;

  public HBaseGraphStore(HTable graphsTable, HTable verticesTable) {
    this.graphsTable = graphsTable;
    this.verticesTable = verticesTable;
  }

  @Override public void writeGraph(final Graph graph) {
    LOG.info("writing: " + graph);
    try {
      // graph id
      Put put = new Put(Bytes.toBytes(graph.getID()));
      // graph labels
      put = writeLabels(put, graph.getLabels());
      // graph properties
      put = writeProperties(put, graph);
      // graph vertices
      put = writeVertices(put, graph.getVertices());

      graphsTable.put(put);
      graphsTable.flushCommits();
    } catch (InterruptedIOException e) {
      e.printStackTrace();
    } catch (RetriesExhaustedWithDetailsException e) {
      e.printStackTrace();
    }
  }

  @Override public void writeVertex(final Vertex vertex) {
    LOG.info("writing: " + vertex);
    try {
      // vertex id
      Put put = new Put(Bytes.toBytes(vertex.getID()));
      // vertex labels
      put = writeLabels(put, vertex.getLabels());
      // vertex properties
      put = writeProperties(put, vertex);
      // outgoing edges
      put = writeEdges(put, CF_OUT_EDGES_BYTES, vertex.getOutgoingEdges());
      // incoming edges
      put = writeEdges(put, CF_IN_EDGES_BYTES, vertex.getIncomingEdges());

      verticesTable.put(put);
      verticesTable.flushCommits();
    } catch (RetriesExhaustedWithDetailsException e) {
      e.printStackTrace();
    } catch (InterruptedIOException e) {
      e.printStackTrace();
    }
  }

  @Override public Graph readGraph(final Long graphID) {
    Graph g = null;
    try {
      byte[] rowKey = Bytes.toBytes(graphID);
      Result res = graphsTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        Iterable<String> labels = readLabels(res);
        Map<String, Object> properties = readProperties(res);
        Iterable<Long> vertices = readVertices(res);
        g = new SimpleGraph(graphID, labels, properties, vertices);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return g;
    }
  }

  @Override public Vertex readVertex(final Long vertexID) {
    Vertex v = null;
    try {
      byte[] rowKey = Bytes.toBytes(vertexID);
      Result res = verticesTable.get(new Get(rowKey));
      if (!res.isEmpty()) {
        Iterable<String> labels = readLabels(res);
        Map<String, Object> properties = readProperties(res);
        Map<String, Map<String, Object>> outEdges = readEdges(res, CF_OUT_EDGES_BYTES);
        Map<String, Map<String, Object>> inEdges = readEdges(res, CF_IN_EDGES_BYTES);
        v = new SimpleVertex(vertexID, labels, properties, outEdges, inEdges);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      return v;
    }
  }

  private Put writeLabels(Put put, Iterable<String> labels) {
    int internalLabelID = 0;
    for (String label : labels) {
      put.add(CF_LABELS_BYTES, Bytes.toBytes(internalLabelID++), Bytes.toBytes(label));
    }
    return put;
  }

  private Iterable<String> readLabels(Result res) {
    List<String> labels = new ArrayList<>();
    for (Map.Entry<byte[], byte[]> labelColumn : res.getFamilyMap(CF_LABELS_BYTES).entrySet()) {
      labels.add(Bytes.toString(labelColumn.getValue()));
    }
    return labels;
  }

  private Put writeProperties(Put put, Attributed attributedObject) {
    for (String key : attributedObject.getPropertyKeys()) {
      put.add(CF_PROPERTIES_BYTES, Bytes.toBytes(key),
          encodeValue(attributedObject.getProperty(key)));
    }
    return put;
  }

  private Map<String, Object> readProperties(Result res) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<byte[], byte[]> propertyColumn : res.getFamilyMap(CF_PROPERTIES_BYTES)
        .entrySet()) {
      properties
          .put(Bytes.toString(propertyColumn.getKey()), decodeValue(propertyColumn.getValue()));
    }
    return properties;
  }

  private Put writeVertices(Put put, Iterable<Long> vertices) {
    for (Long vertex : vertices) {
      put.add(CF_VERTICES_BYTES, Bytes.toBytes(vertex), null);
    }
    return put;
  }

  private Iterable<Long> readVertices(Result res) {
    List<Long> vertices = new ArrayList<>();
    for (Map.Entry<byte[], byte[]> vertexColumn : res.getFamilyMap(CF_VERTICES_BYTES).entrySet()) {
      vertices.add(Bytes.toLong(vertexColumn.getKey()));
    }
    return vertices;
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

  private byte[] encodeValue(Object value) {
    Class<?> valueClass = value.getClass();
    byte[] decodedValue;
    if (valueClass.equals(Boolean.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_BOOLEAN }, Bytes.toBytes((Boolean) value));
    } else if (valueClass.equals(Integer.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_INTEGER }, Bytes.toBytes((Integer) value));
    } else if (valueClass.equals(Long.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_LONG }, Bytes.toBytes((Long) value));
    } else if (valueClass.equals(Float.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_FLOAT }, Bytes.toBytes((Float) value));
    } else if (valueClass.equals(Double.TYPE)) {
      decodedValue = Bytes.add(new byte[] { TYPE_DOUBLE }, Bytes.toBytes((Double) value));
    } else if (valueClass.equals(String.class)) {
      decodedValue = Bytes.add(new byte[] { TYPE_STRING }, Bytes.toBytes((String) value));
    } else {
      throw new IllegalArgumentException(
          valueClass + " not supported by graph store " + HBaseGraphStore.class);
    }
    return decodedValue;
  }

  private Object decodeValue(byte[] encValue) {
    Object o = null;
    if (encValue.length > 0) {
      byte type = encValue[0];
      byte[] value = Bytes.tail(encValue, encValue.length - 1);
      switch (type) {
      case TYPE_BOOLEAN:
        o = Bytes.toBoolean(value);
        break;
      case TYPE_INTEGER:
        o = Bytes.toInt(value);
        break;
      case TYPE_LONG:
        o = Bytes.toLong(value);
        break;
      case TYPE_FLOAT:
        o = Bytes.toFloat(value);
        break;
      case TYPE_DOUBLE:
        o = Bytes.toDouble(value);
        break;
      case TYPE_STRING:
        o = Bytes.toString(value);
        break;
      }
    }
    return o;
  }

  @Override public void close() {
    try {
      graphsTable.close();
      verticesTable.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
