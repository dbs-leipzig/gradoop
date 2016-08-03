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

package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Used to describe entities that can have properties.
 */
public interface EPGMAttributed {

  /**
   * Returns all properties of that entity.
   *
   * @return properties
   */
  PropertyList getProperties();

  /**
   * Returns all property keys of that entity or {@code null} it that entity has
   * no properties.
   *
   * @return property keys
   */
  Iterable<String> getPropertyKeys();

  /**
   * Returns the object referenced by the given key or {@code null} if the key
   * does not exist.
   *
   * @param key property key
   * @return property value or {@code null} if {@code key} does not exist
   */
  PropertyValue getPropertyValue(String key);

  /**
   * Sets the given properties as new properties.
   *
   * @param properties new properties
   */
  void setProperties(PropertyList properties);

  /**
   * Adds a given property to that entity. If a property with the same key
   * does not exist, a new property will be created. Otherwise, the existing
   * property value will be overwritten by the given value.
   *
   * @param property property
   *
   */
  void setProperty(Property property);

  /**
   * Adds a given property to that entity. If {@code key} does not exist, a new
   * property will be created. Otherwise, the existing property value will be
   * overwritten by the given value.
   *
   * @param key   property key
   * @param value property value
   */
  void setProperty(String key, PropertyValue value);

  /**
   * Adds a given property to that entity. If {@code key} does not exist, a new
   * property will be created. Otherwise, the existing property value will be
   * overwritten by the given value.
   *
   * @param key   property key
   * @param value property value
   */
  void setProperty(String key, Object value);

  /**
   * Returns the number of properties stored at that entity.
   *
   * @return number or properties
   */
  int getPropertyCount();

  /**
   * Returns true, if the element has a property with the given property key.
   *
   * @param key property key
   * @return true, if element has property with given key
   */
  boolean hasProperty(String key);
}
