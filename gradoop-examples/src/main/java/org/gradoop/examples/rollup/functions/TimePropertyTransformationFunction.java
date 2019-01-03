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
package org.gradoop.examples.rollup.functions;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.TransformationFunction;

/**
 * Transformation function that creates separate year, month, day, hour and minute properties based
 * on a property of type long.
 *
 * @param <E> element type
 */
@FunctionAnnotation.ForwardedFields("id;label")
public class TimePropertyTransformationFunction<E extends EPGMElement>
  implements TransformationFunction<E> {

  /**
   * serialVersionUID.
   */
  private static final long serialVersionUID = 42L;

  /**
   * Key of the property containing the long value.
   */
  private String propertyKey;

  /**
   * Creates a new instance of TimePropertyTransformationFunction.
   *
   * @param propertyKey key of the property containing the time stamp
   */
  public TimePropertyTransformationFunction(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public E apply(E current, E transformed) {
    PropertyValue timePropertyValue = current.getPropertyValue(propertyKey);

    if (timePropertyValue == null) {
      return current;
    }

    LocalDateTime timeOfCall = LocalDateTime.ofEpochSecond(
      (long) timePropertyValue.getInt(), 0, OffsetDateTime.now().getOffset());

    transformed.setLabel(current.getLabel());

    transformed.setProperty("year", timeOfCall.getYear());
    transformed.setProperty("month", timeOfCall.getMonth().getValue());
    transformed.setProperty("day", timeOfCall.getDayOfMonth());
    transformed.setProperty("hour", timeOfCall.getHour());
    transformed.setProperty("minute", timeOfCall.getMinute());

    return transformed;
  }
}
