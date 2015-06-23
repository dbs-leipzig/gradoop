package org.gradoop.model;

import java.util.Map;

/**
 * Used to describe entities that store a set of properties, where each property
 * is defined by a property key and a property value.
 */
public interface Attributed {

  /**
   * Returns all properties of that entity.
   *
   * @return properties
   */
  Map<String, Object> getProperties();

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
  Object getProperty(String key);

  /**
   * Adds a given property to that entity. If {@code} does not exist, it will be
   * created, if it exists, the value will be overwritten by the given value.
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
}
