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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.tuple.Tuple2ToWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Base class to compute value distributions of computed from EPGM elements.
 *
 * @param <EL> EPGM element type
 * @param <T> value type
 */
abstract class ValueDistribution<EL extends Element, T>
  implements UnaryGraphToValueOperator<DataSet<WithCount<T>>> {

  /**
   * Maps an EPGM element to a value that can be counted.
   */
  private final MapFunction<EL, T> valueFunction;

  /**
   * Constructor
   *
   * @param valueFunction extracts a value from an EPGM element
   */
  ValueDistribution(MapFunction<EL, T> valueFunction) {
    this.valueFunction = valueFunction;
  }

  /**
   * Maps the EPGM element to a value according to the specified {@code valueFunction} and groups
   * those values and counts the values per group.
   *
   * @param elements EPGM elements
   * @return extracted values and the corresponding number of elements with that value
   */
  DataSet<WithCount<T>> compute(DataSet<EL> elements) {
    return Count.groupBy(elements.map(valueFunction))
      .map(new Tuple2ToWithCount<>());
  }
}
