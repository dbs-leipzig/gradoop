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

package org.gradoop.model.api;

import org.apache.hadoop.io.WritableComparable;

/**
 * A single property in the EPGM model. A property consists of a property key
 * and a property value. A given property key does not enforce specific value
 * type.
 */
public interface EPGMProperty extends WritableComparable<EPGMProperty> {

  /**
   * Returns the property key.
   *
   * @return property key
   */
  String getKey();

  /**
   * Sets the property key.
   *
   * @param key property key (must not be {@code null})
   */
  void setKey(String key);

  /**
   * Returns the property value.
   *
   * @return property value
   */
  EPGMPropertyValue getValue();

  /**
   * Sets the property value.
   *
   * @param propertyValue property value  (must not be {@code null})
   */
  void setValue(EPGMPropertyValue propertyValue);

}
