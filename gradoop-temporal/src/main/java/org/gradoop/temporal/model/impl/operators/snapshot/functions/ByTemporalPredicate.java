/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.snapshot.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;

/**
 * A filter function that accepts only elements matching a temporal predicate.
 *
 * @param <T> The temporal element type.
 */
public class ByTemporalPredicate<T extends TemporalElement> implements CombinableFilter<T> {

  /**
   * Condition to be checked.
   */
  private final TemporalPredicate condition;

  /**
   * Specifies the time dimension that will be considered by the predicate.
   */
  private TimeDimension dimension;


  /**
   * Creates a filter instance from a temporal predicate and a time dimension.
   *
   * @param predicate The temporal predicate to check.
   * @param dimension The time dimension that will be considered by the predicate.
   */
  public ByTemporalPredicate(TemporalPredicate predicate, TimeDimension dimension) {
    this.condition = Objects.requireNonNull(predicate, "No predicate given.");
    this.dimension = Objects.requireNonNull(dimension, "No time dimension given.");
  }

  @Override
  public boolean filter(T element) {
    Tuple2<Long, Long> timeValues = element.getTimeByDimension(dimension);
    return condition.test(timeValues.f0, timeValues.f1);
  }
}
