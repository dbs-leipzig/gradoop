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
 * Represents a collection of properties.
 *
 * Properties are ordered by their insertion order.
 */
public interface EPGMProperties
  extends Iterable<EPGMProperty>, WritableComparable<EPGMProperties> {

  /**
   * Returns property keys in insertion order.
   *
   * @return property keys
   */
  Iterable<String> getKeys();

  /**
   * Checks if a property with a given key is contained in the properties.
   *
   * @param key property key
   * @return true, if there is a property with the given key
   */
  boolean hasKey(String key);

  /**
   *
   * @param key
   * @return
   */
  EPGMPropertyValue get(String key);

  void set(EPGMProperty property);

  void set(String key, EPGMPropertyValue value);

  void set(String key, Object value);

  int size();

  boolean isEmpty();

}
