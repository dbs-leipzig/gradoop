/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
