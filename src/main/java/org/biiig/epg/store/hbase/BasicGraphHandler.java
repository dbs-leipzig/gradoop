package org.biiig.epg.store.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.biiig.epg.model.Graph;

/**
 * Created by s1ck on 11/10/14.
 */
public class BasicGraphHandler extends BasicHandler implements GraphHandler {

  private static final byte[] CF_VERTICES_BYTES = Bytes.toBytes(HBaseGraphStore.CF_VERTICES);

  @Override public Put writeVertices(Put put, Graph graph) {
    for (Long vertex : graph.getVertices()) {
      put.add(CF_VERTICES_BYTES, Bytes.toBytes(vertex), null);
    }
    return put;
  }

  @Override public Iterable<Long> readVertices(Result res) {
    return getColumnKeysFromFamiliy(res, CF_VERTICES_BYTES);
  }
}