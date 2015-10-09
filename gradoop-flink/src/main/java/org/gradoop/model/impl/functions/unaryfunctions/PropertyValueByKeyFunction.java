/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.functions.unaryfunctions;

import org.gradoop.model.api.Attributed;
import org.gradoop.model.impl.functions.UnaryFunction;

/**
 * Returns a property value by the given property key.
 *
 * @param <IN>  entity that holds properties
 * @param <OUT> property value type
 */
public class PropertyValueByKeyFunction<IN extends Attributed, OUT>
  implements UnaryFunction<IN, OUT> {

  /**
   * Property value type, needed for type check and cast.
   */
  private final Class<OUT> clazz;

  /**
   * Property key.
   */
  private final String propertyKey;

  /**
   * Creates a new instance using the given arguments.
   *
   * @param clazz       property value type
   * @param propertyKey property key
   */
  public PropertyValueByKeyFunction(Class<OUT> clazz, String propertyKey) {
    if (clazz == null) {
      throw new IllegalArgumentException("clazz must not be null");
    }
    if (propertyKey == null || "".equals(propertyKey)) {
      throw new IllegalArgumentException(
        "propertyKey must not be null or empty");
    }
    this.clazz = clazz;
    this.propertyKey = propertyKey;
  }

  @Override
  public OUT execute(IN entity) throws Exception {
    Object val = entity.getProperty(propertyKey);
    if (val != null && clazz.isInstance(val)) {
      return clazz.cast(val);
    }
    return null;
  }
}
