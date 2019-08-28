/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.diff.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.operators.diff.Diff;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.util.Objects;

/**
 * Evaluates two temporal predicates per element of a data set and sets the
 * {@value Diff#PROPERTY_KEY} accordingly.
 * Values not matching any of the two predicates will be discarded.
 *
 * @param <E> The element type.
 */
@FunctionAnnotation.ReadFields("validTime")
@FunctionAnnotation.NonForwardedFields("properties")
public class DiffPerElement<E extends TemporalElement> implements FlatMapFunction<E, E> {

  /**
   * The predicate used to determine the first snapshot.
   */
  private final TemporalPredicate first;

  /**
   * The predicate used to determine the second snapshot.
   */
  private final TemporalPredicate second;

  /**
   * Create an instance of this function, setting the two temporal predicates used to determine the snapshots.
   *
   * @param first  The predicate used for the first snapshot.
   * @param second The predicate used for the second snapshot.
   */
  public DiffPerElement(TemporalPredicate first, TemporalPredicate second) {
    this.first = Objects.requireNonNull(first);
    this.second = Objects.requireNonNull(second);
  }

  @Override
  public void flatMap(E value, Collector<E> out) {
    Tuple2<Long, Long> validTime = value.getValidTime();
    boolean inFirst = first.test(validTime.f0, validTime.f1);
    boolean inSecond = second.test(validTime.f0, validTime.f1);
    PropertyValue result;
    if (inFirst && inSecond) {
      result = Diff.VALUE_EQUAL;
    } else if (inFirst) {
      result = Diff.VALUE_REMOVED;
    } else if (inSecond) {
      result = Diff.VALUE_ADDED;
    } else {
      return;
    }
    value.setProperty(Diff.PROPERTY_KEY, result);
    out.collect(value);
  }
}
