
package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.api.GraphHeadHandler;
import org.gradoop.common.storage.api.PersistentGraphHead;

/**
 * Creates HBase {@link Mutation} from persistent graph data using graph
 * data handler.
 */
public class BuildGraphHeadMutation extends RichMapFunction
  <PersistentGraphHead, Tuple2<GradoopId, Mutation>> {

  /**
   * Serial version uid.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reusable tuple for each writer.
   */
  private transient Tuple2<GradoopId, Mutation> reuseTuple;

  /**
   * Graph data handler to create Mutations.
   */
  private final GraphHeadHandler<PersistentGraphHead> graphHeadHandler;

  /**
   * Creates rich map function.
   *
   * @param graphHeadHandler graph data handler
   */
  public BuildGraphHeadMutation(GraphHeadHandler<PersistentGraphHead> graphHeadHandler) {
    this.graphHeadHandler = graphHeadHandler;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    reuseTuple = new Tuple2<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, Mutation> map(
    PersistentGraphHead persistentGraphData) throws Exception {
    GradoopId key = persistentGraphData.getId();
    Put put = new Put(graphHeadHandler.getRowKey(persistentGraphData.getId()));
    put = graphHeadHandler.writeGraphHead(put, persistentGraphData);

    reuseTuple.f0 = key;
    reuseTuple.f1 = put;
    return reuseTuple;
  }
}
