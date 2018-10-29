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
 * Converts the jdbc data type to matching EPGM property value if possible
 */
public class PropertyValueParser {

  /**
   * Converts jdbc data type to matching EPGM property value if possible
   *
   * @param att Database value
   * @return Gradoop property value
   */
  public static PropertyValue parse(Object att) {

    PropertyValue propValue = null;

    if (att == null) {
      propValue = PropertyValue.NULL_VALUE;
    } else {
      if (att.getClass() == Date.class) {
        propValue = PropertyValue
            .create(((Date) att).toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
      } else if (att.getClass() == byte[].class) {
        propValue = PropertyValue.create(String.valueOf(att));
      } else {
        propValue = PropertyValue.create(att);
      }
    }
    return propValue;
  }
}
