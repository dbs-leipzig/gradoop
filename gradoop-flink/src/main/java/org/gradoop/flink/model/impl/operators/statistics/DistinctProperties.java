
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.CombinePropertyValueDistribution;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Set;

/**
 * Base class for Statistic operators calculating the number of distinct property values grouped by
 * a given key K
 *
 * @param <T> element type
 * @param <K> grouping key
 */
public abstract class DistinctProperties<T extends GraphElement, K>
  implements UnaryGraphToValueOperator<DataSet<WithCount<K>>> {


  @Override
  public DataSet<WithCount<K>> execute(LogicalGraph graph) {
    return extractValuePairs(graph)
      .groupBy(0)
      .reduceGroup(new CombinePropertyValueDistribution<>());
  }

  /**
   * Extracts key value pairs from the given logical graph
   * @param graph input graph
   * @return key value pairs
   */
  protected abstract DataSet<Tuple2<K, Set<PropertyValue>>> extractValuePairs(LogicalGraph graph);
}
