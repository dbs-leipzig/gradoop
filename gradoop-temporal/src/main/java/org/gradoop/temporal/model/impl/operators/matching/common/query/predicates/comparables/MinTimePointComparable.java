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
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.MinTimePoint}
 */
public class MinTimePointComparable extends TemporalComparable {

  /**
   * The wrapped MinTimePoint
   */
  private MinTimePoint minTimePoint;
  /**
   * Wrappers for the arguments
   */
  private ArrayList<QueryComparable> args;

  /**
   * Creates a new wrapper.
   *
   * @param minTimePoint the wrapped MinTimePoint.
   */
  public MinTimePointComparable(MinTimePoint minTimePoint) {
    this.minTimePoint = minTimePoint;
    args = new ArrayList<>();
    for (TimePoint arg : minTimePoint.getArgs()) {
      args.add(ComparableFactory.createComparableFrom(arg));
    }
  }

  public ArrayList<QueryComparable> getArgs() {
    return args;
  }

  public void setArgs(
    ArrayList<QueryComparable> args) {
    this.args = args;
  }

  public MinTimePoint getMinTimePoint() {
    return minTimePoint;
  }

  public void setMinTimePoint(MinTimePoint minTimePoint) {
    this.minTimePoint = minTimePoint;
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    long min = Long.MAX_VALUE;
    for (QueryComparable arg : args) {
      long argValue = arg.evaluate(embedding, metaData).getLong();
      if (argValue < min) {
        min = argValue;
      }
    }
    return PropertyValue.create(min);
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    if (minTimePoint.getVariables().size() > 1) {
      throw new UnsupportedOperationException("can not evaluate an expression with >1 variable on" +
        " a single GraphElement!");
    }
    long min = Long.MAX_VALUE;
    for (QueryComparable arg : args) {
      long argValue = arg.evaluate(element).getLong();
      if (argValue < min) {
        min = argValue;
      }
    }
    return PropertyValue.create(min);
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

    MinTimePointComparable that = (MinTimePointComparable) o;
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
    return minTimePoint != null ? minTimePoint.hashCode() : 0;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return minTimePoint;
  }
}
