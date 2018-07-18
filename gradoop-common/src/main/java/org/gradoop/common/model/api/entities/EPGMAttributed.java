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
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Property;

import javax.annotation.Nullable;

/**
 * Used to describe entities that can have properties.
 */
public interface EPGMAttributed {

  /**
   * Returns all properties of that entity.
   *
   * @return properties
   */
  @Nullable Properties getProperties();

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
  void setProperties(Properties properties);

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
   * Removes the properties associated with the given key.
   *
   * @param key property key
   * @return associated property value or {@code null} if the key was not found
   */
  PropertyValue removeProperty(String key);

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
