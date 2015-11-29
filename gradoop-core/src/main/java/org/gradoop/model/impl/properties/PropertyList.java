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

package org.gradoop.model.impl.properties;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.gradoop.model.api.EPGMPropertyList;
import org.gradoop.model.api.EPGMProperty;
import org.gradoop.model.api.EPGMPropertyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Default List implementation for a property collection.
 */
public class PropertyList implements EPGMPropertyList {

  /**
   * Internal representation
   */
  private List<EPGMProperty> properties;

  /**
   * Default constructor
   */
  public PropertyList() {
    properties = Lists.newArrayList();
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
    PropertyList properties = new PropertyList();

    if (map != null) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        properties.set(entry.getKey(), PropertyValue.create(entry.getValue()));
      }
    }

    return properties;
  }

  @Override
  public Iterable<String> getKeys() {
    List<String> keys = Lists.newArrayListWithCapacity(size());
    for (EPGMProperty property : properties) {
      keys.add(property.getKey());
    }
    return keys;
  }

  @Override
  public boolean containsKey(String key) {
    return get(key) != null;
  }

  @Override
  public EPGMPropertyValue get(String key) {
    EPGMPropertyValue result = null;
    for (EPGMProperty property : properties) {
      if (property.getKey().equals(key)) {
        result = property.getValue();
        break;
      }
    }
    return result;
  }

  @Override
  public void set(EPGMProperty property) {
    int index = 0;
    for (EPGMProperty epgmProperty : properties) {
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

  @Override
  public void set(String key, EPGMPropertyValue value) {
    set(Property.create(key, value));
  }

  @Override
  public void set(String key, Object value) {
    set(key, PropertyValue.create(value));
  }

  @Override
  public int size() {
    return properties.size();
  }

  @Override
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
  public Iterator<EPGMProperty> iterator() {
    return properties.iterator();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(properties.size());
    for (EPGMProperty property : properties) {
      property.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int propertyCount = dataInput.readInt();
    properties = Lists.newArrayListWithCapacity(propertyCount);
    for (int i = 0; i < propertyCount; i++) {
      EPGMProperty p = new Property();
      p.readFields(dataInput);
      properties.add(p);
    }
  }

  @Override
  public String toString() {
    return String.format("{%s}", StringUtils.join(properties, ", "));
  }
}
