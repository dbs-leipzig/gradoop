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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.time.TimeConstant;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.TimeConstant}
 */
public class TimeConstantComparable extends TemporalComparable {

  /**
   * The wrapped constant
   */
  private final TimeConstant constant;

  /**
   * Creates a new wrapper for a TimeConstant.
   *
   * @param constant the TimeConstant to wrap
   */
  public TimeConstantComparable(TimeConstant constant) {
    this.constant = constant;
  }

  /**
   * Returns the wrapped constant
   *
   * @return the wrapped constant
   */
  public TimeConstant getTimeConstant() {
    return constant;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return constant;
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    return PropertyValue.create(constant.evaluate().get());
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    return PropertyValue.create(constant.evaluate().get());
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return new HashSet<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeConstantComparable that = (TimeConstantComparable) o;

    return that.constant.equals(constant);

  }

  @Override
  public int hashCode() {
    return constant.hashCode();
  }

  public TimeConstant getConstant() {
    return constant;
  }

}
