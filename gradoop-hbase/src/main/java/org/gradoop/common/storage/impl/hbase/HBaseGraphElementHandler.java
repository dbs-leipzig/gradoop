
package org.gradoop.common.storage.impl.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.GraphElementHandler;
import org.gradoop.common.util.HBaseConstants;

import java.io.IOException;

/**
 * Handler class for entities that are contained in logical graphs (i.e.,
 * vertex and edge data).
 */
public abstract class HBaseGraphElementHandler extends
  HBaseElementHandler implements GraphElementHandler {
  /**
   * Byte representation of the graphs column identifier.
   */
  private static final byte[] COL_GRAPHS_BYTES =
    Bytes.toBytes(HBaseConstants.COL_GRAPHS);

  /**
   * {@inheritDoc}
   */
  @Override
  public Put writeGraphIds(Put put, EPGMGraphElement graphElement) throws
    IOException {

    if (graphElement.getGraphCount() > 0) {
      put = put.add(CF_META_BYTES, COL_GRAPHS_BYTES, graphElement.getGraphIds().toByteArray());
    }

    return put;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopIdList readGraphIds(Result res) throws IOException {
    byte[] graphBytes = res.getValue(CF_META_BYTES, COL_GRAPHS_BYTES);

    GradoopIdList graphIds;

    if (graphBytes != null) {
      graphIds = GradoopIdList.fromByteArray(graphBytes);
    } else {
      graphIds = new GradoopIdList();
    }

    return graphIds;
  }
}
