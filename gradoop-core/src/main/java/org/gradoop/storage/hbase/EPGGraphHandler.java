package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.GConstants;
import org.gradoop.model.Graph;
import org.gradoop.model.impl.GraphFactory;

/**
 * Handles storing graphs in a HBase table.
 */
public class EPGGraphHandler extends BasicHandler implements GraphHandler {
  /**
   * Byte array representation of the vertices column family.
   */
  private static final byte[] CF_VERTICES_BYTES =
    Bytes.toBytes(GConstants.CF_VERTICES);

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getRowKey(final Long graphID) {
    if (graphID == null) {
      throw new IllegalArgumentException("graphID must not be null");
    }
    return Bytes.toBytes(graphID.toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getGraphID(byte[] rowKey) {
    if (rowKey == null) {
      throw new IllegalArgumentException("rowKey must not be null");
    }
    return Long.valueOf(Bytes.toString(rowKey));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeVertices(Put put, Graph graph) {
    for (Long vertex : graph.getVertices()) {
      put.add(CF_VERTICES_BYTES, Bytes.toBytes(vertex), null);
    }
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeGraph(final Put put, final Graph graph) {
    writeLabels(put, graph);
    writeProperties(put, graph);
    writeVertices(put, graph);
    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Long> readVertices(Result res) {
    return getColumnKeysFromFamiliy(res, CF_VERTICES_BYTES);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph readGraph(Result res) {
    return GraphFactory
      .createDefaultGraph(Long.valueOf(Bytes.toString(res.getRow())),
        readLabels(res), readProperties(res), readVertices(res));
  }
}
