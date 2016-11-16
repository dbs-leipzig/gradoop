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

package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the properties of an {@link org.gradoop.common.model.impl.pojo.Element}.
 */
public class Properties implements Iterable<Property>, Writable, Serializable {

  /**
   * Default capacity for new property lists.
   */
  private static final int DEFAULT_CAPACITY = 10;

  /**
   * Internal representation
   */
  private Map<String, PropertyValue> properties;

  /**
   * Default constructor
   */
  public Properties() {
    properties = new HashMap<>(DEFAULT_CAPACITY);
  }

  /**
   * Creates property list with given capacity.
   *
   * @param capacity initial capacity
   */
  private Properties(int capacity) {
    properties = new HashMap<>(capacity);
  }

  /**
   * Creates a new property list.
   *
   * @return PropertyList
   */
  public static Properties create() {
    return new Properties();
  }

  /**
   * Creates a new property list with the given initial capacity.
   *
   * @param capacity initial capacity
   * @return PropertyList
   */
  public static Properties createWithCapacity(int capacity) {
    return new Properties(capacity);
  }

  /**
   * Creates a new property collection from a given map.
   *
   * If map is {@code null} an empty properties instance will be returned.
   *
   * @param map key value map
   * @return PropertyList
   */
  public static Properties createFromMap(Map<String, Object> map) {
    Properties properties;

    if (map == null) {
      properties = Properties.createWithCapacity(0);
    } else {
      properties = Properties.createWithCapacity(map.size());

      for (Map.Entry<String, Object> entry : map.entrySet()) {
        properties.set(entry.getKey(), PropertyValue.create(entry.getValue()));
      }
    }

    return properties;
  }

  /**
   * Returns property keys in insertion order.
   *
   * @return property keys
   */
  public Iterable<String> getKeys() {
    return properties.keySet();
  }

  /**
   * Checks if a property with the given key is contained in the properties.
   *
   * @param key property key
   * @return true, if there is a property with the given key
   */
  public boolean containsKey(String key) {
    return get(key) != null;
  }

  /**
   * Returns the value to the given key of {@code null} if the value does not
   * exist.
   *
   * @param key property key
   * @return property value or {@code null} if key does not exist
   */
  public PropertyValue get(String key) {
    Objects.requireNonNull(key);
    return properties.get(key);
  }

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param property property
   */
  public void set(Property property) {
    set(property.getKey(), property.getValue());
  }

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param key   property key
   * @param value property value
   */
  public void set(String key, PropertyValue value) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(value);
    properties.put(key, value);
  }

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param key   property key
   * @param value property value
   */
  public void set(String key, Object value) {
    PropertyValue propertyValue;

    if (value instanceof PropertyValue) {
      propertyValue = (PropertyValue) value;
    } else {
      propertyValue = PropertyValue.create(value);
    }

    set(key, propertyValue);
  }

  /**
   * Removes the property of the given key from the list.
   *
   * @param key property key
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   */
  public PropertyValue remove(String key) {
    Objects.requireNonNull(key);
    return properties.remove(key);
  }

  /**
   * Removes the given property from the list.
   *
   * @param property property
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
   */
  public PropertyValue remove(Property property) {
    Objects.requireNonNull(property);
    return remove(property.getKey());
  }

  /**
   * Returns the number of properties.
   *
   * @return number of properties
   */
  public int size() {
    return properties.size();
  }

  /**
   * True, if the properties collection does not store any properties.
   *
   * @return true if collection is empty, false otherwise
   */
  public boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Two properties collections are considered equal, if they contain the
   * same properties in the same order.
   *
   * @param o other properties collection
   * @return  true, iff the two property collections contain the same elements
   *          in the same order
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Properties that = (Properties) o;

    return !(properties != null ? !properties.equals(that.properties) :
      that.properties != null);
  }

  /**
   * Two properties collections have identical hash codes, if they contain the
   * same properties in the same order.
   *
   * @return hash code
   */
  @Override
  public int hashCode() {
    return properties != null ? properties.hashCode() : 0;
  }

  @Override
  public Iterator<Property> iterator() {
    return properties.entrySet().stream()
      .map(e -> Property.create(e.getKey(), e.getValue()))
      .collect(Collectors.toList()).iterator();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(properties.size());

    for (Map.Entry<String, PropertyValue> entry : properties.entrySet()) {
      dataOutput.writeUTF(entry.getKey());
      entry.getValue().write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int propertyCount = dataInput.readInt();
    this.properties = new HashMap<>(propertyCount);

    String key;
    PropertyValue value;

    for (int i = 0; i < propertyCount; i++) {
      key = dataInput.readUTF();
      value = new PropertyValue();
      value.readFields(dataInput);
      properties.put(key, value);
    }
  }

  @Override
  public String toString() {
    return properties.entrySet().stream()
      .map(e -> Property.create(e.getKey(), e.getValue()).toString())
      .collect(Collectors.joining(","));
  }
}
