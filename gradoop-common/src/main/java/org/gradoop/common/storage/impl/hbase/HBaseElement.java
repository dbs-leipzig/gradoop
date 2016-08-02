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

package org.gradoop.common.storage.impl.hbase;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * Wraps EPGM data entity.
 *
 * @param <T> entity type
 */
public abstract class HBaseElement<T extends EPGMElement>
  implements EPGMElement {

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
  public PropertyList getProperties() {
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
  public void setProperties(PropertyList properties) {
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
