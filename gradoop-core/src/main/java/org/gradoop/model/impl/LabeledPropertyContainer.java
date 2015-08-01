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

import org.gradoop.model.Identifiable;
import org.gradoop.model.Labeled;

import java.util.Map;

/**
 * Abstract entity that holds a single labels and properties.
 */
public abstract class LabeledPropertyContainer extends
  PropertyContainer implements Identifiable, Labeled {

  /**
   * Entity identifier.
   */
  protected final Long id;

  /**
   * Label of that entity.
   */
  protected String label;

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         entity identifier
   * @param label      label
   * @param properties key-value-map
   */
  protected LabeledPropertyContainer(Long id, String label,
    Map<String, Object> properties) {
    super(properties);
    this.id = id;
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(Long id) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLabel(String label) {
    this.label = label;
  }
}
