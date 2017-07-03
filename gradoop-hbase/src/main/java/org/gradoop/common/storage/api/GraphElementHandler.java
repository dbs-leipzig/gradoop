
package org.gradoop.common.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.io.IOException;

/**
 * Responsible for reading and writing graph element entity data from and to
 * HBase. These are usually vertex and edge data objects.
 */
public interface GraphElementHandler extends ElementHandler {
  /**
   * Adds the given graph identifiers to the given {@link Put} and returns it.
   *
   * @param put          {@link Put} to add graph identifiers to
   * @param graphElement graph element
   * @return put with graph identifiers
   */
  Put writeGraphIds(final Put put, final EPGMGraphElement graphElement) throws
    IOException;

  /**
   * Reads the graph identifiers from the given {@link Result}.
   *
   * @param res HBase row
   * @return graphs identifiers
   */
  GradoopIdList readGraphIds(final Result res) throws IOException;
}
