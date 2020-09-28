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
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.TimeLiteral}
 */
public class TimeLiteralComparable extends TemporalComparable {

  /**
   * The wrapped TimeLiteral
   */
  private final TimeLiteral timeLiteral;

  /**
   * Creates a new wrapper
   *
   * @param timeLiteral the wrapped literal
   */
  public TimeLiteralComparable(TimeLiteral timeLiteral) {
    this.timeLiteral = timeLiteral;
  }

  /**
   * Returns the wrapped {@link TimeLiteral}
   *
   * @return wrapped literal
   */
  public TimeLiteral getTimeLiteral() {
    return timeLiteral;
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    return PropertyValue.create(timeLiteral.evaluate().get());
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    return PropertyValue.create(
      timeLiteral.evaluate().get());
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return new HashSet<>(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeLiteralComparable that = (TimeLiteralComparable) o;

    return that.timeLiteral.equals(timeLiteral);
  }

  @Override
  public int hashCode() {
    return timeLiteral != null ? timeLiteral.hashCode() : 0;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return timeLiteral;
  }
}
