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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.MaxTimePoint}
 */
public class MaxTimePointComparable extends TemporalComparable {

  /**
   * The wrapped MaxTimePoint
   */
  private final MaxTimePoint maxTimePoint;

  /**
   * Wrappers for the arguments
   */
  private final ArrayList<QueryComparable> args;

  /**
   * Creates a new wrapper.
   *
   * @param maxTimePoint the wrapped MaxTimePoint.
   */
  public MaxTimePointComparable(MaxTimePoint maxTimePoint) {
    this.maxTimePoint = maxTimePoint;
    args = new ArrayList<>();
    for (TimePoint arg : maxTimePoint.getArgs()) {
      args.add(ComparableFactory.createComparableFrom(arg));
    }
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    long max = Long.MIN_VALUE;
    for (QueryComparable arg : args) {
      long argValue = arg.evaluate(embedding, metaData).getLong();
      if (argValue > max) {
        max = argValue;
      }
    }
    return PropertyValue.create(max);
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    if (maxTimePoint.getVariables().size() > 1) {
      throw new UnsupportedOperationException("can not evaluate an expression with >1 variable on" +
        " a single GraphElement!");
    }
    long max = Long.MIN_VALUE;
    for (QueryComparable arg : args) {
      long argValue = arg.evaluate(element).getLong();
      if (argValue > max) {
        max = argValue;
      }
    }
    return PropertyValue.create(max);
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    HashSet<String> keys = new HashSet<>();
    for (QueryComparable comp: getArgs()) {
      keys.addAll(comp.getPropertyKeys(variable));
    }
    return keys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MaxTimePointComparable that = (MaxTimePointComparable) o;
    if (that.args.size() != args.size()) {
      return false;
    }

    for (QueryComparable arg : args) {
      boolean foundMatch = false;
      for (QueryComparable candidate : that.args) {
        if (arg.equals(candidate)) {
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return maxTimePoint != null ? maxTimePoint.hashCode() : 0;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return maxTimePoint;
  }

  public MaxTimePoint getMaxTimePoint() {
    return maxTimePoint;
  }

  public ArrayList<QueryComparable> getArgs() {
    return args;
  }

}

