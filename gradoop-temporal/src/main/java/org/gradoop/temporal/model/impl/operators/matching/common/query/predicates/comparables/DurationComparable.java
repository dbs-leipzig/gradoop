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
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util.ComparableFactory;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimePoint;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.time.Duration}
 */
public class DurationComparable extends TemporalComparable {

  /**
   * The wrapped duration
   */
  private final Duration duration;
  /**
   * the from value of the interval
   */
  private final TemporalComparable from;
  /**
   * the to value of the interval
   */
  private final TemporalComparable to;
  /**
   * Long describing the system time of the query
   */
  private final Long now;

  /**
   * Creates a new wrapper
   *
   * @param duration the Duration to be wrapped
   */
  public DurationComparable(Duration duration) {
    this(duration, new TimeLiteral("now"));
  }

  /**
   * Creates a new wrapper
   *
   * @param duration the duration to be wrapped
   * @param now system time of the query
   */
  public DurationComparable(Duration duration, TimeLiteral now) {
    this.duration = duration;
    this.from = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getFrom());
    this.to = (TemporalComparable) ComparableFactory.createComparableFrom(duration.getTo());
    this.now = now.getMilliseconds();
  }

  public Duration getDuration() {
    return duration;
  }

  public TemporalComparable getFrom() {
    return from;
  }

  public TemporalComparable getTo() {
    return to;
  }

  public Long getNow() {
    return now;
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    Long toLong = to.evaluate(embedding, metaData).getLong() == Long.MAX_VALUE ?
      now : to.evaluate(embedding, metaData).getLong();
    return PropertyValue.create(toLong -
      from.evaluate(embedding, metaData).getLong());
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    Long toLong = to.evaluate(element).getLong() == Long.MAX_VALUE ?
      now : to.evaluate(element).getLong();
    return PropertyValue.create(toLong -
      from.evaluate(element).getLong());
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    HashSet<String> keys = new HashSet<>();
    keys.addAll(from.getPropertyKeys(variable));
    keys.addAll(to.getPropertyKeys(variable));
    return keys;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return duration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DurationComparable that = (DurationComparable) o;
    return that.from.equals(from) && that.to.equals(to);
  }

  @Override
  public int hashCode() {
    return duration.hashCode();
  }
}
