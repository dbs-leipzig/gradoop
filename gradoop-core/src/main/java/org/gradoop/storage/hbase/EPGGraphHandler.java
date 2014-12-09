package org.gradoop.storage.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.GConstants;
import org.gradoop.model.Graph;
import org.gradoop.model.inmemory.MemoryGraph;

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
    return new MemoryGraph(
      Bytes.toLong(res.getRow()),
      readLabels(res),
      readProperties(res),
      readVertices(res));
  }
}