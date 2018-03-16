/**
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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;


import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extracts a {@link PropertyValue} from a given entity using a given
 * property key. If the EPGM element does not have this property,
 * the property value will be a new property value, containing the specified
 * default element.
 *
 * @param <EL> EPGM element
 */
public class GetPropertyValue<EL extends Element>
  implements MapFunction<EL, Tuple1<PropertyValue>> {

  /**
   * Property key to retrieve property values
   */
  private final String propertyKey;

  /**
   * Instance of Number, containing 0 of the same type as
   * the property value
   */
  private final Number defaultValue;

  /**
   * Constructor
   *
   * @param propertyKey property key to retrieve values for
   * @param defaultValue user defined default
   */
  public GetPropertyValue(String propertyKey, Number defaultValue) {
    this.propertyKey = checkNotNull(propertyKey);
    this.defaultValue = defaultValue;
  }

  @Override
  public Tuple1<PropertyValue> map(EL entity) throws Exception {
    if (entity.hasProperty(propertyKey)) {
      return new Tuple1<>(entity.getPropertyValue(propertyKey));
    }
    return new Tuple1<>(PropertyValue.create(defaultValue));
  }
}
