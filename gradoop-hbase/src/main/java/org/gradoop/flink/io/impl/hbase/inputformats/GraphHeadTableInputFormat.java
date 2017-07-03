
package org.gradoop.flink.io.impl.hbase.inputformats;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.util.HBaseConstants;

/**
 * Reads graph data from HBase.
 *
 * @param <G> EPGM graph head type
 */
public class GraphHeadTableInputFormat<G extends EPGMGraphHead>
  extends TableInputFormat<Tuple1<G>> {

  /**
   * Handles reading of persistent graph data.
   */
  private final GraphHeadHandler<G> graphHeadHandler;

  /**
   * Table to read from.
   */
  private final String graphHeadTableName;

  /**
   * Creates an graph table input format.
   *
   * @param graphHeadHandler   graph data handler
   * @param graphHeadTableName graph data table name
   */
  public GraphHeadTableInputFormat(GraphHeadHandler<G> graphHeadHandler,
    String graphHeadTableName) {
    this.graphHeadHandler = graphHeadHandler;
    this.graphHeadTableName = graphHeadTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Scan getScanner() {
    Scan scan = new Scan();
    scan.setCaching(HBaseConstants.HBASE_DEFAULT_SCAN_CACHE_SIZE);
    return scan;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getTableName() {
    return graphHeadTableName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Tuple1<G> mapResultToTuple(Result result) {
    return new Tuple1<>(graphHeadHandler.readGraphHead(result));
  }
}
