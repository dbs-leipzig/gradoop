
package org.gradoop.flink.model.impl.operators.difference;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.base.SetOperatorBase;
import org.gradoop.flink.model.impl.operators.difference.functions.CreateTuple2WithLong;
import org.gradoop.flink.model.impl.operators.difference.functions.IdOf0InTuple2;
import org.gradoop.flink.model.impl.operators.difference.functions.RemoveCut;

/**
 * Returns a collection with all logical graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 *
 * @see DifferenceBroadcast
 */
public class Difference extends SetOperatorBase {

  /**
   * Computes the logical graph dataset for the resulting collection.
   *
   * @return logical graph dataset of the resulting collection
   */
  @Override
  protected DataSet<GraphHead> computeNewGraphHeads() {
    // assign 1L to each logical graph in the first collection
    DataSet<Tuple2<GraphHead, Long>> thisGraphs = firstCollection
      .getGraphHeads()
      .map(new CreateTuple2WithLong<GraphHead>(1L));
    // assign 2L to each logical graph in the second collection
    DataSet<Tuple2<GraphHead, Long>> otherGraphs = secondCollection
      .getGraphHeads()
      .map(new CreateTuple2WithLong<GraphHead>(2L));

    // union the logical graphs, group them by their identifier and check that
    // there is no graph in the group that belongs to the second collection
    return thisGraphs
      .union(otherGraphs)
      .groupBy(new IdOf0InTuple2<GraphHead, Long>())
      .reduceGroup(new RemoveCut<GraphHead>());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Difference.class.getName();
  }
}
