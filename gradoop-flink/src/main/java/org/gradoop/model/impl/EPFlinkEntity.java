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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Maps;
import org.gradoop.model.Attributed;
import org.gradoop.model.Identifiable;
import org.gradoop.model.Labeled;

import java.util.Map;
import java.util.Objects;

public abstract class EPFlinkEntity implements Identifiable, Attributed,
  Labeled, Comparable<EPFlinkEntity> {

  private Long id;

  private String label;

  private Map<String, Object> properties;

  public EPFlinkEntity() {
    this.properties = Maps.newHashMap();
  }

  public EPFlinkEntity(EPFlinkEntity otherEntity) {
    this.id = otherEntity.getId();
    this.label = otherEntity.getLabel();
    this.properties = Maps.newHashMap(otherEntity.getProperties());
  }

  public EPFlinkEntity(Long id, String label, Map<String, Object> properties) {
    this.id = id;
    this.label = label;
    if (properties != null) {
      this.properties = properties;
    } else {
      this.properties = Maps.newHashMap();
    }
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public void setId(Long id) {
    this.id = id;
  }

  @Override
  public String getLabel() {
    return label;
  }

  @Override
  public void setLabel(String label) {
    this.label = label;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return properties.keySet();
  }

  @Override
  public Object getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public <T> T getProperty(String key, Class<T> type) {
    return type.cast(properties.get(key));
  }

  @Override
  public void setProperty(String key, Object value) {
    properties.put(key, value);
  }

  @Override
  public int getPropertyCount() {
    return properties.size();
  }

  @Override
  public int compareTo(EPFlinkEntity o) {
    return Long.compare(this.getId(), o.getId());
  }

  @Override
  public String toString() {
    return "EPFlinkEntity{" +
      "id=" + id +
      ", label='" + label + '\'' +
      ", properties=" + properties +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EPFlinkEntity)) {
      return false;
    }
    EPFlinkEntity that = (EPFlinkEntity) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
