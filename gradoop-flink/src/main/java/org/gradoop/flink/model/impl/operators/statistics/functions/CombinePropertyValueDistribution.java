
package org.gradoop.flink.model.impl.operators.statistics.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;
import java.util.Set;

/**
 * This UDF counts the number of distinct property values grouped by a key K
 *
 * Reduces <K, Set<PropertyValue>> --> <K, Long>
 * @param <K> the grouping key
 */
public class CombinePropertyValueDistribution<K> implements
  GroupCombineFunction<Tuple2<K, Set<PropertyValue>>, Tuple2<K, Set<PropertyValue>>>,
  GroupReduceFunction<Tuple2<K, Set<PropertyValue>>, WithCount<K>> {

  @Override
  public void combine(Iterable<Tuple2<K, Set<PropertyValue>>> values,
    Collector<Tuple2<K, Set<PropertyValue>>> out) throws Exception {

    List<Tuple2<K, Set<PropertyValue>>> pairs = Lists.newArrayList(values);
    out.collect(Tuple2.of(pairs.get(0).f0, combineSets(pairs)));
  }

  @Override
  public void reduce(Iterable<Tuple2<K, Set<PropertyValue>>> values,
    Collector<WithCount<K>> collector) throws Exception {

    List<Tuple2<K, Set<PropertyValue>>> pairs = Lists.newArrayList(values);
    collector.collect(new WithCount<>(pairs.get(0).f0, (long) combineSets(pairs).size()));
  }

  /**
   * Extracts sets of property values from tuples and combines them into one set
   * @param values list of <K, Set<PropertyValue>> tuples
   * @return combined set of all PropertyValues
   */
  private Set<PropertyValue> combineSets(List<Tuple2<K, Set<PropertyValue>>> values) {
    return values.stream()
      .map(triple -> triple.f1)
      .reduce((lhs, rhs) -> {
          lhs.addAll(rhs);
          return lhs;
        })
      .get();
  }
}
