package org.gradoop.storage.hbase;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.gradoop.GConstants;
import org.gradoop.model.Edge;
import org.gradoop.model.GraphElement;
import org.gradoop.model.inmemory.MemoryEdge;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by s1ck on 11/8/14.
 */
public class EPGVertexHandler extends BasicHandler
  implements VertexHandler {
  private static Logger LOG =
    Logger.getLogger(EPGVertexHandler.class);

  private static final byte[] CF_OUT_EDGES_BYTES =
    Bytes.toBytes(GConstants.CF_OUT_EDGES);
  private static final byte[] CF_IN_EDGES_BYTES =
    Bytes.toBytes(GConstants.CF_IN_EDGES);
  private static final byte[] CF_GRAPHS_BYTES =
    Bytes.toBytes(GConstants.CF_GRAPHS);

  private static final String PROPERTY_TOKEN_SEPARATOR_STRING = " ";
  private static final Pattern PROPERTY_TOKEN_SEPARATOR_PATTERN = Pattern.compile(" ");
  private static final String EDGE_KEY_TOKEN_SEPARATOR_STRING = ".";
  private static final Pattern EDGE_KEY_TOKEN_SEPARATOR_PATTERN =
    Pattern.compile("\\.");

  @Override
  public void createVerticesTable(final HBaseAdmin admin,
                                  final HTableDescriptor tableDescriptor)
    throws IOException {
    LOG.info("creating table " + tableDescriptor.getNameAsString());
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_LABELS));
    tableDescriptor
      .addFamily(new HColumnDescriptor(GConstants.CF_PROPERTIES));
    tableDescriptor
      .addFamily(new HColumnDescriptor(GConstants.CF_OUT_EDGES));
    tableDescriptor
      .addFamily(new HColumnDescriptor(GConstants.CF_IN_EDGES));
    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_GRAPHS));
    admin.createTable(tableDescriptor);
  }

  @Override
  public Put writeOutgoingEdges(final Put put, final Iterable<? extends Edge>
    edges) {
    return writeEdges(put, CF_OUT_EDGES_BYTES, edges);
  }

  @Override
  public Put writeIncomingEdges(final Put put,
                                final Iterable<? extends Edge> edges) {
    return writeEdges(put, CF_IN_EDGES_BYTES, edges);
  }

  @Override
  public Put writeGraphs(final Put put, final GraphElement vertex) {
    for (Long graphID : vertex.getGraphs()) {
      put.add(CF_GRAPHS_BYTES, Bytes.toBytes(graphID), null);
    }
    return put;
  }

  @Override
  public Iterable<Edge> readOutgoingEdges(final Result res) {
    return readEdges(res, CF_OUT_EDGES_BYTES);
  }

  @Override
  public Iterable<Edge> readIncomingEdges(final Result res) {
    return readEdges(res, CF_IN_EDGES_BYTES);
  }

  @Override
  public Iterable<Long> readGraphs(final Result res) {
    return getColumnKeysFromFamiliy(res, CF_GRAPHS_BYTES);
  }

  private Put writeEdges(Put put, final byte[] columnFamily,
                         final Iterable<? extends Edge> edges) {
    for (Edge edge : edges) {
      put = writeEdge(put, columnFamily, edge);
    }
    return put;
  }

  private Put writeEdge(final Put put, final byte[] columnFamily,
                        final Edge edge) {
    String edgeKey = createEdgeKey(edge);
    byte[] edgeKeyBytes = Bytes.toBytes(edgeKey);
    String properties = createEdgePropertiesString(edge);
    byte[] propertiesBytes = Bytes.toBytes(properties);
    put.add(columnFamily, edgeKeyBytes, propertiesBytes);
    return put;
  }

  private String createEdgeKey(final Edge edge) {
    return String.format("%s%s%d%s%d",
      edge.getLabel(),
      EDGE_KEY_TOKEN_SEPARATOR_STRING,
      edge.getOtherID(),
      EDGE_KEY_TOKEN_SEPARATOR_STRING,
      edge.getIndex());
  }

  private String createEdgePropertiesString(final Edge edge) {
    String result = "";
    Iterable<String> propertyKeys = edge.getPropertyKeys();
    if (propertyKeys != null) {
      final List<String> propertyStrings = Lists.newArrayList();
      for (String propertyKey : propertyKeys) {
        Object propertyValue = edge.getProperty(propertyKey);
        String propertyString = String.format("%s%s%d%s%s",
          propertyKey,
          PROPERTY_TOKEN_SEPARATOR_STRING,
          getType(propertyValue),
          PROPERTY_TOKEN_SEPARATOR_STRING,
          propertyValue);
        propertyStrings.add(propertyString);
      }
      result = Joiner.on(PROPERTY_TOKEN_SEPARATOR_STRING).join
        (propertyStrings);
    }
    return result;
  }

  private Iterable<Edge> readEdges(final Result res,
                                   final byte[] columnFamily) {
    final List<Edge> edges = Lists.newArrayList();
    for (Map.Entry<byte[], byte[]> edgeColumn : res.getFamilyMap(columnFamily)
      .entrySet()) {
      String edgeKey = Bytes.toString(edgeColumn.getKey());
      Map<String, Object> edgeProperties = new HashMap<>();
      String propertyString = Bytes.toString(edgeColumn.getValue());
      if (propertyString.length() > 0) {
        String[] tokens = PROPERTY_TOKEN_SEPARATOR_PATTERN.split(propertyString);
        for (int i = 0; i < tokens.length; i += 3) {
          String propertyKey = tokens[i];
          byte propertyType = Byte.parseByte(tokens[i + 1]);
          Object propertyValue =
            decodeValueFromString(propertyType, tokens[i + 2]);
          edgeProperties.put(propertyKey, propertyValue);
        }
      }
      edges.add(readEdge(edgeKey, edgeProperties));
    }
    return edges;
  }

  private Edge readEdge(final String edgeKey, final Map<String,
    Object> properties) {
    String[] keyTokens = EDGE_KEY_TOKEN_SEPARATOR_PATTERN.split(edgeKey);
    String edgeLabel = keyTokens[0];
    Long otherID = Long.valueOf(keyTokens[1]);
    Long edgeIndex = Long.valueOf(keyTokens[2]);
    return new MemoryEdge(otherID, edgeLabel, edgeIndex, properties);
  }
}
