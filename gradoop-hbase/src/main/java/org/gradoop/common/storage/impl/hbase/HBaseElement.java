/**
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
package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Property;

/**
 * Wraps EPGM data entity.
 *
 * @param <T> entity type
 */
public abstract class HBaseElement<T extends EPGMElement> implements EPGMElement {

  /**
   * Encapsulated EPGM element.
   */
  private T epgmElement;

  /**
   * Creates an persistent EPGM element.
   *
   * @param epgmElement runtime data entity
   */
  protected HBaseElement(T epgmElement) {
    this.epgmElement = epgmElement;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getProperties() {
    return epgmElement.getProperties();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getPropertyKeys() {
    return epgmElement.getPropertyKeys();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PropertyValue getPropertyValue(String key) {
    return epgmElement.getPropertyValue(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperties(Properties properties) {
    epgmElement.setProperties(properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(Property property) {
    epgmElement.setProperty(property);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, PropertyValue value) {
    epgmElement.setProperty(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, Object value) {
    epgmElement.setProperty(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PropertyValue removeProperty(String key) {
    return epgmElement.removeProperty(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasProperty(String key) {
    return getProperties() != null && getProperties().containsKey(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPropertyCount() {
    return epgmElement.getPropertyCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getId() {
    return epgmElement.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(GradoopId id) {
    epgmElement.setId(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return epgmElement.getLabel();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLabel(String label) {
    epgmElement.setLabel(label);
  }

  /**
   * Returns the encapsulated EPGM element.
   *
   * @return EPGM element
   */
  public T getEpgmElement() {
    return epgmElement;
  }

  /**
   * Sets the encapsulated EPGM element.
   *
   * @param epgmElement EPGM element
   */
  public void setEpgmElement(T epgmElement) {
    this.epgmElement = epgmElement;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseElement{");
    sb.append("epgmElement=").append(epgmElement);
    sb.append('}');
    return sb.toString();
  }


}
