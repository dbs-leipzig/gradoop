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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.util;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Provides utility methods that are needed in several binning classes
 */
public class Util {
  /**
   * Converts a numerical PropertyValue to its value as a double
   *
   * @param value PropertyValue to convert
   * @param cls   type of the PropertyValue
   * @return double representation of value
   */
  public static double propertyValueToDouble(PropertyValue value, Class cls) {
    if (cls.equals(Integer.class)) {
      return value.getInt();
    }
    if (cls.equals(Double.class)) {
      return value.getDouble();
    }
    if (cls.equals(Long.class)) {
      return (double) value.getLong();
    }
    if (cls.equals(Float.class)) {
      return Double.parseDouble(value.toString());
    }
    return 0.;
  }

  /**
   * Converts a numerical PropertyValue to its value as a double
   * @param value PropertyValue to convert
   * @return double representation of value
   */
  public static double propertyValueToDouble(PropertyValue value) {
    return propertyValueToDouble(value, value.getType());
  }
}
