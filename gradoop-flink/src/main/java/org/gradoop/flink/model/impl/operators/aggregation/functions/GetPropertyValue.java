/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
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
