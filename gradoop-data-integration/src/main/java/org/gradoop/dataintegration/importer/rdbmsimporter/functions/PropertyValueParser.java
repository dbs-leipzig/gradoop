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

package org.gradoop.dataintegration.importer.rdbmsimporter.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;

import java.time.ZoneId;
import java.util.Date;

/**
 * Converts the JDBC data type of a database tuple value to a fitting {@link PropertyValue}.
 */
class PropertyValueParser {

  /**
   * No need to instantiate this class.
   */
  private PropertyValueParser() { }

  /**
   * Converts jdbc data type to matching EPGM property value if possible.
   *
   * @param value value of database tuple
   * @return gradoop property value
   */
  static PropertyValue parse(Object value) {

    PropertyValue propValue;

    if (value == null) {
      propValue = PropertyValue.NULL_VALUE;
    } else {
      if (value.getClass() == Date.class) {
        propValue = PropertyValue
          .create(((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
      } else if (value.getClass() == byte[].class) {
        propValue = PropertyValue.create(String.valueOf(value));
      } else {
        propValue = PropertyValue.create(value);
      }
    }
    return propValue;
  }
}
