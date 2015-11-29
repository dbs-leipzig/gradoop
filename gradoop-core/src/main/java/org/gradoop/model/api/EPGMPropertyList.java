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

import org.apache.hadoop.io.Writable;

/**
 * Represents a list of properties.
 *
 * Properties inside property lists are ordered by their insertion order. A
 * property list cannot contain two properties with the same key.
 *
 * Two property lists are considered equal, if they contain the same
 * properties in the same order.
 *
 */
public interface EPGMPropertyList
  extends Iterable<EPGMProperty>, Writable {

  /**
   * Returns property keys in insertion order.
   *
   * @return property keys
   */
  Iterable<String> getKeys();

  /**
   * Checks if a property with the given key is contained in the properties.
   *
   * @param key property key
   * @return true, if there is a property with the given key
   */
  boolean containsKey(String key);

  /**
   * Returns the value to the given key of {@code null} if the value does not
   * exist.
   *
   * @param key property key
   * @return propert value or {@code null} if key does not exist
   */
  EPGMPropertyValue get(String key);

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param property property
   */
  void set(EPGMProperty property);

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param key   property key
   * @param value property value
   */
  void set(String key, EPGMPropertyValue value);

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param key   property key
   * @param value property value
   */
  void set(String key, Object value);

  /**
   * Returns the number of properties.
   *
   * @return number of properties
   */
  int size();

  /**
   * True, if the properties collection does not store any properties.
   *
   * @return true if collection is empty, false otherwise
   */
  boolean isEmpty();

}
