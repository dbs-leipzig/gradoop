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
package org.gradoop.common.model.impl.properties;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the properties of an {@link org.gradoop.common.model.impl.pojo.Element}.
 */
public class Properties implements Iterable<Property>, Value, Serializable {

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
   * Removes all elements from these properties.
   */
  public void clear() {
    this.properties.clear();
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Property> iterator() {
    return toList().iterator();
  }

  /**
   * Returns a list of properties.
   *
   * @return List of properties
   */
  public List<Property> toList() {
    return  properties.entrySet().stream()
            .map(e -> Property.create(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutputView outputView) throws IOException {
    outputView.writeInt(properties.size());

    for (Map.Entry<String, PropertyValue> entry : properties.entrySet()) {
      outputView.writeUTF(entry.getKey());
      entry.getValue().write(outputView);
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void read(DataInputView inputView) throws IOException {
    int propertyCount = inputView.readInt();
    this.properties = new HashMap<>(propertyCount);

    String key;
    PropertyValue value;

    for (int i = 0; i < propertyCount; i++) {
      key = inputView.readUTF();
      value = new PropertyValue();
      value.read(inputView);
      properties.put(key, value);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return toList().stream()
      .map(Property::toString)
      .collect(Collectors.joining(","));
  }
}
