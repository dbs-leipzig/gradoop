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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Properties implements EPGMProperties {

  private Collection<Property> properties;

  public Properties() {
    properties = Lists.newArrayList();
  }

  public Collection<Property> getProperties() {
    return properties;
  }

  public void setProperties(List<Property> properties) {
    this.properties = properties;
  }

  /**
   * TODO key cache, value cache
   *
   * @param key
   * @return
   */
  @Override
  public boolean hasKey(String key) {
    return get(key) != null;
  }

  @Override
  public EPGMPropertyValue get(String key) {
    EPGMPropertyValue result = null;
    for (Property property : properties) {
      if (property.getKey().equals(key)) {
        result = property.getValue();
        break;
      }
    }
    return result;
  }

  @Override
  public void set(EPGMProperty property) {

  }

  @Override
  public void set(String key, Object value) {

  }

  @Override
  public void set(String key, Integer value) {

  }

  @Override
  public Iterable<String> getKeys() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int compareTo(EPGMProperties o) {
    return 0;
  }

  @Override
  public Iterator<EPGMProperty> iterator() {
    return null;
  }

  public static EPGMProperties createfromMap(Map<String, Object> map) {
    EPGMProperties properties = new Properties();

    for(Map.Entry<String, Object> entry : map.entrySet()) {
      properties.set(entry.getKey(), entry.getValue());
    }

    return properties;
  }
}
