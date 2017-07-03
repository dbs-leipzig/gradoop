
package org.gradoop.flink.io.impl.hbase.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.storage.api.PersistentGraphHead;
import org.gradoop.common.storage.api.PersistentGraphHeadFactory;

/**
 * Creates persistent graph data from graph data and vertex/edge identifiers.
 *
 * @param <G> EPGM graph head type
 */
public class BuildPersistentGraphHead<G extends EPGMGraphHead>
  implements JoinFunction
  <Tuple3<GradoopId, GradoopIdList, GradoopIdList>, G, PersistentGraphHead> {

  /**
   * Persistent graph data factory.
   */
  private PersistentGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates join function.
   *
   * @param graphHeadFactory persistent graph data factory
   */
  public BuildPersistentGraphHead(
    PersistentGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistentGraphHead join(
    Tuple3<GradoopId, GradoopIdList, GradoopIdList> longSetSetTuple3, G graphHead)
      throws Exception {
    return graphHeadFactory.createGraphHead(graphHead, longSetSetTuple3.f1,
      longSetSetTuple3.f2);
  }
}
