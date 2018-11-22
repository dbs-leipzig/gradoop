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
package org.gradoop.examples.rollup;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.api.functions.TransformationFunction;

/**
 * Transformation function that creates separate year, month, day, hour and minute properties based
 * on a property of type long.
 *
 * @param <E> gradoop element
 */
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

  /**
   * Runs the transformation.
   */
  @Override
  public E apply(E current, E transformed) {
    Date timeOfCall = new Date(current.getPropertyValue(propertyKey).getInt() * 1000L);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(timeOfCall);

    transformed.setProperty("year", calendar.get(Calendar.YEAR));
    transformed.setProperty("month", calendar.get(Calendar.MONTH));
    transformed.setProperty("day", calendar.get(Calendar.DAY_OF_MONTH));
    transformed.setProperty("hour", calendar.get(Calendar.HOUR));
    transformed.setProperty("minute", calendar.get(Calendar.MINUTE));

    return transformed;
  }
}
