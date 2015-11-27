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

import java.util.Iterator;
import java.util.List;

public class PropertiesPojo implements Properties {

  private List<PropertyPojo> properties;

  public PropertiesPojo() {
    properties = Lists.newArrayList();
  }

  public List<PropertyPojo> getProperties() {
    return properties;
  }

  public void setProperties(List<PropertyPojo> properties) {
    this.properties = properties;
  }

  @Override
  public boolean hasKey(String key) {
    return false;
  }

  @Override
  public Property getProperty(String key) {
    return null;
  }

  @Override
  public void setProperty(Property property) {

  }

  @Override
  public void setProperty(String key, Object value) {

  }

  @Override
  public void setProperty(String key, Integer value) {

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
  public int compareTo(Properties o) {
    return 0;
  }

  @Override
  public Iterator<Property> iterator() {
    return null;
  }
}
