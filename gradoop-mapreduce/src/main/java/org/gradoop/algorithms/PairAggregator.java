package org.gradoop.algorithms;

import org.apache.hadoop.hbase.util.Pair;
import org.gradoop.io.formats.PairWritable;

/**
 * Used to aggregate the result in the reduce step of
 * {@link org.gradoop.algorithms.SelectAndAggregate}
 */
public interface PairAggregator {
  /**
   * First element states if the graph fulfills the predicate defined for that
   * job, second element is the aggregated {@link Double} value for that graph.
   *
   * @param values result of map phase in
   *               {@link org.gradoop.algorithms.SelectAndAggregate}
   * @return predicate result and aggregated graph value
   */
  Pair<Boolean, Double> aggregate(Iterable<PairWritable> values);
}
