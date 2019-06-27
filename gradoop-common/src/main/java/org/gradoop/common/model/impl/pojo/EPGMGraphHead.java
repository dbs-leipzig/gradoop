/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.pojo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Tuple Implementation of an EPGM graph head.
 *
 * 0: GraphHeadId
 * 1: Label
 * 2: Properties
 */
public class EPGMGraphHead extends Tuple3<GradoopId, String, Properties> implements GraphHead {

  /**
   * Default constructor.
   */
  public EPGMGraphHead() {
    initProperties();
  }

  /**
   * Creates a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph properties
   */
  public EPGMGraphHead(final GradoopId id, final String label,
    final Properties properties) {
    super(id, label, properties);
    initProperties();
  }


  @Nullable
  @Override
  public Properties getProperties() {
    return this.f2;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return (this.f2 != null) ? this.f2.getKeys() : null;
  }

  @Override
  public PropertyValue getPropertyValue(String key) {
    return (this.f2 != null) ? this.f2.get(key) : null;
  }

  @Override
  public void setProperties(Properties properties) {
    this.f2 = properties;
  }

  @Override
  public void setProperty(Property property) {
    Preconditions.checkNotNull(property, "Property was null");
    initProperties();
    this.f2.set(property);
  }

  @Override
  public void setProperty(String key, PropertyValue value) {
    initProperties();
    this.f2.set(key, value);
  }

  @Override
  public void setProperty(String key, Object value) {
    initProperties();
    this.f2.set(key, value);
  }

  @Override
  public PropertyValue removeProperty(String key) {
    return (this.f2 != null) ? this.f2.remove(key) : null;
  }

  @Override
  public int getPropertyCount() {
    return (this.f2 != null) ? this.f2.size() : 0;
  }

  @Override
  public boolean hasProperty(String key) {
    return this.f2 != null && this.f2.containsKey(key);
  }

  @Override
  public GradoopId getId() {
    return this.f0;
  }

  @Override
  public void setId(GradoopId id) {
    this.f0 = id;
  }

  @Override
  public String getLabel() {
    return this.f1;
  }

  @Override
  public void setLabel(String label) {
    this.f1 = label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EPGMGraphHead that = (EPGMGraphHead) o;

    return Objects.equals(getId(), that.getId());
  }

  @Override
  public int hashCode() {
    int result = getId().hashCode();
    result = 31 * result + getId().hashCode();
    return result;
  }

  /**
   * Initializes the internal properties field if necessary.
   */
  private void initProperties() {
    if (this.f2 == null) {
      this.f2 = Properties.create();
    }
  }
}
