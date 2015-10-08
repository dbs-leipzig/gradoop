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

package org.gradoop.storage.impl.hbase;

import org.gradoop.model.api.Element;

import java.util.Map;

/**
 * Wraps EPGM data entity.
 *
 * @param <T> entity type
 */
public abstract class PersistentEPGMElement<T extends Element> implements
  Element {

  /**
   * Encapsulated EPGM element.
   */
  private T epgmElement;

  /**
   * Default constructor.
   */
  protected PersistentEPGMElement() {
  }

  /**
   * Creates an persistent EPGM element.
   *
   * @param epgmElement runtime data entity
   */
  protected PersistentEPGMElement(T epgmElement) {
    this.epgmElement = epgmElement;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Object> getProperties() {
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
  public Object getProperty(String key) {
    return epgmElement.getProperty(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <S> S getProperty(String key, Class<S> type) {
    return epgmElement.getProperty(key, type);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperties(Map<String, Object> properties) {
    epgmElement.setProperties(properties);
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
  public int getPropertyCount() {
    return epgmElement.getPropertyCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getId() {
    return epgmElement.getId();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(Long id) {
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
    final StringBuilder sb = new StringBuilder("PersistentEPGMElement{");
    sb.append("epgmElement=").append(epgmElement);
    sb.append('}');
    return sb.toString();
  }
}
