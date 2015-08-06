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
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Logger;
//import org.gradoop.GConstants;
//import org.gradoop.model.GraphData;
//import org.gradoop.model.impl.GraphFactory;
//
//import java.io.IOException;
//import java.util.Set;
//
///**
// * Used to read/write an EPG graph from/to a HBase table.
// */
//public class EPGGraphHandler extends BasicHandler implements GraphHandler {
//  /**
//   * Class logger.
//   */
//  private static Logger LOG = Logger.getLogger(EPGGraphHandler.class);
//
//  /**
//   * Byte array representation of the vertices column family.
//   */
//  private static final byte[] CF_VERTICES_BYTES =
//    Bytes.toBytes(GConstants.CF_VERTICES);
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public byte[] getRowKey(final Long graphID) {
//    if (graphID == null) {
//      throw new IllegalArgumentException("graphID must not be null");
//    }
//    return Bytes.toBytes(graphID.toString());
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Long getGraphID(final byte[] rowKey) {
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
//  public Put writeVertices(final Put put, final GraphData graphData) {
//    for (Long vertex : graphData.getVertices()) {
//      put.add(CF_VERTICES_BYTES, Bytes.toBytes(vertex), null);
//    }
//    return put;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Put writeGraph(final Put put, final GraphData graphData) {
//    writeLabel(put, graphData);
//    writeProperties(put, graphData);
//    writeVertices(put, graphData);
//    return put;
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public Set<Long> readVertices(final Result res) {
//    return getColumnKeysFromFamily(res, CF_VERTICES_BYTES);
//  }
//
//  /**
//   * {@inheritDoc}
//   */
//  @Override
//  public GraphData readGraph(final Result res) {
//    return GraphFactory
//      .createDefaultGraph(Long.valueOf(Bytes.toString(res.getRow())),
//        readLabel(res), readProperties(res), readVertices(res));
//  }
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
//    tableDescriptor.addFamily(new HColumnDescriptor(GConstants.CF_VERTICES));
//    admin.createTable(tableDescriptor);
//  }
//}
