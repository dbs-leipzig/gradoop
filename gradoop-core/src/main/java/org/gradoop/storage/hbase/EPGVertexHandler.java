///*
// * This file is part of Gradoop.
// *
// * Gradoop is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * Gradoop is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.gradoop.storage.hbase;
//
//import com.google.common.base.Joiner;
//import com.google.common.collect.Lists;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Logger;
//import org.gradoop.GConstants;
//import org.gradoop.model.EdgeData;
//import org.gradoop.model.GraphElement;
//import org.gradoop.model.VertexData;
//import org.gradoop.model.impl.DefaultEdgeData;
//import org.gradoop.model.impl.EdgeFactory;
//import org.gradoop.model.impl.VertexFactory;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.regex.Pattern;
//
///**
// * Used to read/write an EPG vertex from/to a HBase table.
// */
//public class EPGVertexHandler extends BasicHandler implements VertexHandler {
//  /**
//   * Logger
//   */
//  private static Logger LOG = Logger.getLogger(EPGVertexHandler.class);
//
//  /**
//   * Byte array representation of the outgoing edges column family.
//   */
//  private static final byte[] CF_OUT_EDGES_BYTES =
//    Bytes.toBytes(GConstants.CF_OUT_EDGES);
//  /**
//   * Byte array representation of the incoming edges column family.
//   */
//  private static final byte[] CF_IN_EDGES_BYTES =
//    Bytes.toBytes(GConstants.CF_IN_EDGES);
//  /**
//   * Byte array representation of the graphs column family.
//   */
//  private static final byte[] CF_GRAPHS_BYTES =
//    Bytes.toBytes(GConstants.CF_GRAPHS);
//
//  /**
//   * Separates a property string into tokens.
//   */
//  private static final String PROPERTY_TOKEN_SEPARATOR_STRING = " ";
//  /**
//   * Separates a property string into tokens.
//   */
//  private static final Pattern PROPERTY_TOKEN_SEPARATOR_PATTERN =
//    Pattern.compile(" ");
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public void createTable(final HBaseAdmin admin,
//    final HTableDescriptor tableDescriptor) throws IOException {
//    LOG.info("creating table " + tableDescriptor.getNameAsString());
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_META));
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_PROPERTIES));
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_OUT_EDGES));
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_IN_EDGES));
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_GRAPHS));
//    admin.createTable(tableDescriptor);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public byte[] getRowKey(final Long vertexID) {
//    if (vertexID == null) {
//      throw new IllegalArgumentException("vertexID must not be null");
//    }
//    return Bytes.toBytes(vertexID.toString());
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Long getVertexID(final byte[] rowKey) {
//    if (rowKey == null) {
//      throw new IllegalArgumentException("rowKey must not be null");
//    }
//    return Long.valueOf(Bytes.toString(rowKey));
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Put writeOutgoingEdges(final Put put,
//    final Iterable<? extends EdgeData> edges) {
//    return writeEdges(put, CF_OUT_EDGES_BYTES, edges, true);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Put writeIncomingEdges(final Put put,
//    final Iterable<? extends EdgeData> edges) {
//    return writeEdges(put, CF_IN_EDGES_BYTES, edges, false);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Put writeGraphs(final Put put, final GraphElement graphElement) {
//    for (Long graphID : graphElement.getGraphs()) {
//      put.add(CF_GRAPHS_BYTES, Bytes.toBytes(graphID), null);
//    }
//    return put;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Put writeVertex(final Put put, final VertexData vertexData) {
//    writeLabel(put, vertexData);
//    writeProperties(put, vertexData);
//    writeOutgoingEdges(put, vertexData.getOutgoingEdges());
//    writeIncomingEdges(put, vertexData.getIncomingEdges());
//    writeGraphs(put, vertexData);
//    return put;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Iterable<EdgeData> readOutgoingEdges(final Result res) {
//    return readEdges(res, CF_OUT_EDGES_BYTES);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Iterable<EdgeData> readIncomingEdges(final Result res) {
//    return readEdges(res, CF_IN_EDGES_BYTES);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Iterable<Long> readGraphs(final Result res) {
//    return getColumnKeysFromFamily(res, CF_GRAPHS_BYTES);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public VertexData readVertex(final Result res) {
//    return VertexFactory
//      .createDefaultVertex(Long.valueOf(Bytes.toString(res.getRow())),
//        readLabel(res), readProperties(res), readOutgoingEdges(res),
//        readIncomingEdges(res), readGraphs(res));
//  }
//
//  /**
//   * Adds edges to the the given HBase put.
//   *
//   * @param put          {@link org.apache.hadoop.hbase.client.Put} to
//   *                     write the
//   *                     edges to
//   * @param columnFamily CF where the edges shall be stored
//   * @param edges        edges to store
//   * @param isOutgoing   true, if the edge is an outgoing edge, false if
//   *                     incoming
//   * @return the updated put
//   */
//  private Put writeEdges(Put put, final byte[] columnFamily,
//    final Iterable<? extends EdgeData> edges, boolean isOutgoing) {
//    if (edges != null) {
//      for (EdgeData edgeData : edges) {
//        put = writeEdge(put, columnFamily, edgeData, isOutgoing);
//      }
//    }
//    return put;
//  }
//
//  /**
//   * Writes a single edge to a given put.
//   *
//   * @param put          {@link org.apache.hadoop.hbase.client.Put} to
//   *                     write the
//   *                     edge to
//   * @param columnFamily CF where the edges shall be stored
//   * @param edgeData     edge to store
//   * @param isOutgoing   true, if the edge is an outgoing edge, false if
//   *                     incoming
//   * @return the updated put
//   */
//  private Put writeEdge(final Put put, final byte[] columnFamily,
//    final EdgeData edgeData, boolean isOutgoing) {
//    byte[] edgeKey = createEdgeIdentifier(edgeData, isOutgoing);
//    String properties = createEdgePropertiesString(edgeData);
//    byte[] propertiesBytes = Bytes.toBytes(properties);
//    put.add(columnFamily, edgeKey, propertiesBytes);
//    return put;
//  }
//
//  /**
//   * Serializes an edge to an edge identifier in the following format:
//   * <p/>
//   * <edge-identifier> ::= <otherID><index><label>
//   *
//   * @param edgeData   edge to create identifier for
//   * @param isOutgoing true, if the edge is an outgoing edge, false if
//   *                   incoming
//   * @return string representation of the edge identifier
//   */
//  private byte[] createEdgeIdentifier(final EdgeData edgeData,
//    boolean isOutgoing) {
//    byte[] labelBytes = Bytes.toBytes(edgeData.getLabel());
//    byte[] edgeKey = new byte[2 * Bytes.SIZEOF_LONG + labelBytes.length];
//    Bytes.putLong(edgeKey, 0,
//      (isOutgoing) ? edgeData.getTargetVertex() : edgeData.getSourceVertex());
//    Bytes.putLong(edgeKey, Bytes.SIZEOF_LONG, edgeData.getId());
//    Bytes.putBytes(edgeKey, Bytes.SIZEOF_LONG * 2, labelBytes, 0,
//      labelBytes.length);
//    return edgeKey;
//  }
//
//  /**
//   * Creates a string representation of edge properties which are stored as
//   * column value for the edge identifier.
//   *
//   * @param edgeData edge to create property string for
//   * @return string representation of the edge properties
//   */
//  private String createEdgePropertiesString(final EdgeData edgeData) {
//    String result = "";
//    Iterable<String> propertyKeys = edgeData.getPropertyKeys();
//    if (propertyKeys != null) {
//      final List<String> propertyStrings = Lists.newArrayList();
//      for (String propertyKey : propertyKeys) {
//        Object propertyValue = edgeData.getProperty(propertyKey);
//        String propertyString = String
//          .format("%s%s%d%s%s", propertyKey, PROPERTY_TOKEN_SEPARATOR_STRING,
//            getType(propertyValue), PROPERTY_TOKEN_SEPARATOR_STRING,
//            propertyValue);
//        propertyStrings.add(propertyString);
//      }
//      result = Joiner.on(PROPERTY_TOKEN_SEPARATOR_STRING).join(propertyStrings);
//    }
//    return result;
//  }
//
//  /**
//   * Reads edges from a given HBase row result.
//   *
//   * @param res          {@link org.apache.hadoop.hbase.client.Result} to read
//   *                     edges from
//   * @param columnFamily column family where the edges are stored
//   * @return edges
//   */
//  private Iterable<EdgeData> readEdges(final Result res,
//    final byte[] columnFamily) {
//    final List<EdgeData> edgeDatas = Lists.newArrayList();
//    for (Map.Entry<byte[], byte[]> edgeColumn : res.getFamilyMap(columnFamily)
//      .entrySet()) {
//      byte[] edgeKey = edgeColumn.getKey();
//      Map<String, Object> edgeProperties = null;
//      String propertyString = Bytes.toString(edgeColumn.getValue());
//      if (propertyString.length() > 0) {
//        edgeProperties = new HashMap<>();
//        String[] tokens =
//          PROPERTY_TOKEN_SEPARATOR_PATTERN.split(propertyString);
//        for (int i = 0; i < tokens.length; i += 3) {
//          String propertyKey = tokens[i];
//          byte propertyType = Byte.parseByte(tokens[i + 1]);
//          Object propertyValue =
//            decodeValueFromString(propertyType, tokens[i + 2]);
//          edgeProperties.put(propertyKey, propertyValue);
//        }
//      }
//      edgeDatas.add(readEdge(edgeKey, edgeProperties));
//    }
//    return edgeDatas;
//  }
//
//  /**
//   * Creates an edge object based on the given key and properties. The given
//   * edge key is deserialized and used to create a new {@link
//   * DefaultEdgeData} instance.
//   *
//   * @param edgeKey    string representation of edge key
//   * @param properties key-value-map
//   * @return Edge object
//   */
//  private EdgeData readEdge(final byte[] edgeKey,
//    final Map<String, Object> properties) {
//    Long otherID = Bytes.toLong(edgeKey);
//    Long edgeIndex = Bytes.toLong(edgeKey, Bytes.SIZEOF_LONG);
//    String edgeLabel = Bytes.toString(edgeKey, 2 * Bytes.SIZEOF_LONG,
//      edgeKey.length - (2 * Bytes.SIZEOF_LONG));
//    return EdgeFactory
//      .createDefaultEdge(otherID, edgeLabel, edgeIndex, properties);
//  }
//}
