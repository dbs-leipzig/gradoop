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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
public class PropertyList implements Iterable<Property>, Writable {

  /**
   * Default capacity for new property lists.
   */
  private static final int DEFAULT_CAPACITY = 10;

  /**
   * Internal representation
   */
  private List<Property> properties;

  /**
   * Default constructor
   */
  public PropertyList() {
    properties = Lists.newArrayListWithCapacity(DEFAULT_CAPACITY);
  }

  /**
   * Creates property list with given capacity.
   *
   * @param capacity initial capacity
   */
  private PropertyList(int capacity) {
    properties = Lists.newArrayListWithCapacity(capacity);
  }

  /**
   * Creates a new property list.
   *
   * @return PropertyList
   */
  public static PropertyList create() {
    return new PropertyList();
  }

  /**
   * Creates a new property list with the given initial capacity.
   *
   * @param capacity initial capacity
   * @return PropertyList
   */
  public static PropertyList createWithCapacity(int capacity) {
    return new PropertyList(capacity);
  }

  /**
   * Creates a new property collection from a given map.
   *
   * If map is {@code null} an empty properties instance will be returned.
   *
   * @param map key value map
   * @return PropertyList
   */
  public static PropertyList createFromMap(Map<String, Object> map) {
    PropertyList properties;

    if (map == null) {
      properties = PropertyList.createWithCapacity(0);
    } else {
      properties = PropertyList.createWithCapacity(map.size());

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
    List<String> keys = Lists.newArrayListWithCapacity(size());
    for (Property property : properties) {
      keys.add(property.getKey());
    }
    return keys;
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
   * @return propert value or {@code null} if key does not exist
   */
  public PropertyValue get(String key) {
    PropertyValue result = null;
    for (Property property : properties) {
      if (property.getKey().equals(key)) {
        result = property.getValue();
        break;
      }
    }
    return result;
  }

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param property property
   */
  public void set(Property property) {
    int index = 0;
    for (Property epgmProperty : properties) {
      if (epgmProperty.getKey().equals(property.getKey())) {
        break;
      }
      index++;
    }
    if (index >= size()) {
      properties.add(property);
    } else {
      properties.set(index, property);
    }
  }

  /**
   * Sets the given property. If a property with the same property key already
   * exists, it will be replaced by the given property.
   *
   * @param key   property key
   * @param value property value
   */
  public void set(String key, PropertyValue value) {
    set(Property.create(key, value));
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

    PropertyList that = (PropertyList) o;

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
    return properties.iterator();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(properties.size());
    for (Property property : properties) {
      property.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int propertyCount = dataInput.readInt();
    properties = Lists.newArrayListWithCapacity(propertyCount);
    for (int i = 0; i < propertyCount; i++) {
      Property p = new Property();
      p.readFields(dataInput);
      properties.add(p);
    }
  }

  @Override
  public String toString() {
    return StringUtils.join(properties, ",");
  }

}
