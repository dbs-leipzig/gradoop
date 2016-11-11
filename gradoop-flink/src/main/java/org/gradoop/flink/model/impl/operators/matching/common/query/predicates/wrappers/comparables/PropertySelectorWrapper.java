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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.comparables;

import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions
  .MissingElementException;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .ComparableWrapper;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.util.Map;

/**
 * Wraps a {@link PropertySelector}
 */
public class PropertySelectorWrapper extends ComparableWrapper {
  /**
   * Holds the wrapped property selector
   */
  private final PropertySelector propertySelector;

  /**
   * Creates a new wrapper
   * @param propertySelector the wrapped property selector
   */
  public PropertySelectorWrapper(PropertySelector propertySelector) {
    this.propertySelector = propertySelector;
  }

  /**
   * Returns the specified property as property value
   *
   * @param values mapping of variables to embedding entries
   * @return the property value
   * @throws MissingElementException if element is not in mapping
   */
  @Override
  public PropertyValue evaluate(Map<String, EmbeddingEntry> values) {
    EmbeddingEntry entry = values.get(propertySelector.getVariable());

    if(entry == null) throw new MissingElementException(propertySelector.getVariable());

    return entry.getProperties().
      orElse(new PropertyList()).
      get(propertySelector.getPropertyName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PropertySelectorWrapper that = (PropertySelectorWrapper) o;

    return propertySelector != null ? propertySelector.equals(that.propertySelector) :
      that.propertySelector == null;

  }

  @Override
  public int hashCode() {
    return propertySelector != null ? propertySelector.hashCode() : 0;
  }
}
