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
import org.gradoop.model.api.EPGMProperties;
import org.gradoop.model.api.EPGMProperty;
import org.gradoop.model.api.EPGMPropertyValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Default implementation for a property collection.
 */
public class Properties implements EPGMProperties {

  /**
   * Internal representation
   */
  private List<EPGMProperty> properties;

  /**
   * Default constructor
   */
  public Properties() {
    properties = Lists.newArrayList();
  }

  /**
   * Creates a new property collection from a given map.
   *
   * If map is {@code null} an empty properties instance will be returned.
   *
   * @param map key value map
   * @return Properties
   */
  public static EPGMProperties createFromMap(Map<String, Object> map) {
    EPGMProperties properties = new Properties();

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

  /**
   * TODO key cache, value cache
   *
   */
  @Override
  public boolean hasKey(String key) {
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
    set(new Property(key, value));
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

  @Override
  public int compareTo(EPGMProperties o) {
    return 0;
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
}
