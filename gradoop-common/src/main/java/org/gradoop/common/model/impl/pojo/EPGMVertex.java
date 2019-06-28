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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Tuple Implementation of an EPGM vertex.
 *
 * 0: VertexId
 * 1: Label
 * 2: Properties
 * 3: GraphIdSet
 */
public class EPGMVertex extends Tuple4<GradoopId, String, Properties, GradoopIdSet> implements Vertex {

  /**
   * Default constructor.
   */
  public EPGMVertex() {
    initId();
    initProperties();
    initGradoopIdSet();
  }

  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphs     graphs that vertex is contained in
   */
  public EPGMVertex(final GradoopId id, final String label,
    final Properties properties, final GradoopIdSet graphs) {
    super(id, label, properties, graphs);
    initProperties();
    initGradoopIdSet();
  }

  @Override
  public GradoopIdSet getGraphIds() {
    return this.f3;
  }

  @Override
  public void addGraphId(GradoopId graphId) {
    if (this.f3 == null) {
      this.f3 = new GradoopIdSet();
    }
    this.f3.add(graphId);
  }

  @Override
  public void setGraphIds(GradoopIdSet graphIds) {
    this.f3 = graphIds;
  }

  @Override
  public void resetGraphIds() {
    if (this.f3 != null) {
      this.f3.clear();
    }
  }

  @Override
  public int getGraphCount() {
    return (this.f3 != null) ? this.f3.size() : 0;
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
    return (this.f2 != null) ?  this.f2.remove(key) : null;
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
  public String toString() {
    return String.format("%s%s%s{%s}",
      f0,
      f1 == null || f1.equals("") ? "" : ":",
      f1,
      f2 == null ? "" : f2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EPGMVertex that = (EPGMVertex) o;

    return Objects.equals(f0, that.getId());
  }

  @Override
  public int hashCode() {
    int result = getId().hashCode();
    result = 31 * result + getId().hashCode();
    return result;
  }

  /**
   * Initializes the vertex id
   */
  private void initId() {
    if (this.f0 == null) {
      this.f0 = GradoopId.NULL_VALUE;
    }
  }

  /**
   * Initializes the internal properties field.
   */
  private void initProperties() {
    if (this.f2 == null) {
      this.f2 = Properties.create();
    }
  }

  /**
   * Initializes the internal gradoop id set field.
   */
  private void initGradoopIdSet() {
    if (this.f3 == null) {
      this.f3 = GradoopIdSet.create();
    }
  }
}
