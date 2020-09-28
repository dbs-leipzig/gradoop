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
import org.gradoop.temporal.model.impl.pojo.TemporalGraphElement;
import org.s1ck.gdl.model.comparables.time.TimePoint;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps an {@link org.s1ck.gdl.model.comparables.time.TimeSelector}
 */
public class TimeSelectorComparable extends TemporalComparable {

  /**
   * The wrapped TimeSelector
   */
  private final TimeSelector timeSelector;

  /**
   * Creates a new wrapper
   *
   * @param timeSelector the wrapped literal
   */
  public TimeSelectorComparable(TimeSelector timeSelector) {
    this.timeSelector = timeSelector;
  }

  /**
   * Returns the variable of the wrapped TimeSelector
   *
   * @return variable of the TimeSelector
   */
  public String getVariable() {
    return timeSelector.getVariable();
  }

  /**
   * Returns the TimeField (i.e. TX_FROM, TX_TO, VAL_FROM or VAL_TO) of the wrapped TimeSelector
   *
   * @return TimeField of wrapped TimeSelector
   */
  public TimeSelector.TimeField getTimeField() {
    return timeSelector.getTimeProp();
  }

  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    String selectorString = timeSelector.getTimeProp().toString();
    int column = metaData
      .getPropertyColumn(timeSelector.getVariable(), selectorString);

    return embedding.getProperty(column);
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    // here no distinction from global selectors, as element is always a single vertex or edge
    Long time = -1L;
    TimeSelector.TimeField field = timeSelector.getTimeProp();
    if (field.equals(TimeSelector.TimeField.TX_FROM)) {
      time = ((TemporalGraphElement) element).getTxFrom();
    } else if (field == TimeSelector.TimeField.TX_TO) {
      time = ((TemporalGraphElement) element).getTxTo();
    } else if (field == TimeSelector.TimeField.VAL_FROM) {
      time = ((TemporalGraphElement) element).getValidFrom();
    } else if (field == TimeSelector.TimeField.VAL_TO) {
      time = ((TemporalGraphElement) element).getValidTo();
    }
    return PropertyValue.create(time);
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return getVariable().equals(variable) ?
      new HashSet<>(Collections.singletonList(getTimeField().toString())) :
      new HashSet<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeSelectorComparable that = (TimeSelectorComparable) o;

    return that.timeSelector.equals(timeSelector);
  }

  @Override
  public int hashCode() {
    return timeSelector != null ? timeSelector.hashCode() : 0;
  }


  @Override
  public TimePoint getWrappedComparable() {
    return timeSelector;
  }
}
