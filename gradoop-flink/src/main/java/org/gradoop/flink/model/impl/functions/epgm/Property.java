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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Accepts all elements which have a property with the specified key or key value combination.
 *
 * @param <E> EPGM element
 */
public class Property<E extends EPGMElement>
  implements FilterFunction<E> {
  /**
   * PropertyKey to be filtered on.
   */
  private String key;
  /**
   * PropertyValue to be filtered on.
   */
  private PropertyValue value;

  /**
   * Valued constructor, accepts all elements containing a property with the given key.
   *
   * @param key property key
   */
  public Property(String key) {
    this(key, null);
  }

  /**
   * Valued constructor, accepts all elements containing the given property.
   *
   * @param property property, containing of key and value
   */
  public Property(org.gradoop.common.model.impl.properties.Property property) {
    this(property.getKey(), property.getValue());
  }

  /**
   * Valued constructor, accepts all elements containing a property with the given key and the
   * corresponding value.
   *
   * @param key property key
   * @param value property value
   */
  public Property(String key, PropertyValue value) {
    this.key = key;
    this.value = value;
  }


  @Override
  public boolean filter(E e) throws Exception {
    if (e.hasProperty(key)) {
      if (value != null) {
        if (e.getPropertyValue(key).equals(value)) {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }
}
