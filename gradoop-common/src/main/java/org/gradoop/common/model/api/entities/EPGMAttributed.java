
package org.gradoop.common.model.api.entities;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Property;

/**
 * Used to describe entities that can have properties.
 */
public interface EPGMAttributed {

  /**
   * Returns all properties of that entity.
   *
   * @return properties
   */
  Properties getProperties();

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
