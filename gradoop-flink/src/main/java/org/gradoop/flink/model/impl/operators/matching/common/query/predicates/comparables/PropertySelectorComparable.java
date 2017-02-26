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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables;

import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingElementException;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingPropertyException;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link PropertySelector}
 */
public class PropertySelectorComparable extends QueryComparable {
  /**
   * Holds the wrapped property selector
   */
  private final PropertySelector propertySelector;

  /**
   * Creates a new wrapper
   * @param propertySelector the wrapped property selector
   */
  public PropertySelectorComparable(PropertySelector propertySelector) {
    this.propertySelector = propertySelector;
  }

  /**
   * Returns the specified property as property value
   *
   * @param embedding the embedding holding the data
   * @param metaData meta data describing the embedding
   * @return the property value
   * @throws MissingElementException if element is not in mapping
   * @throws MissingPropertyException if property is not in projection
   */
  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    int entryColumn = metaData.getEntryColumn(propertySelector.getVariable());

    if (entryColumn == -1) {
      throw new MissingElementException(propertySelector.getVariable());
    }

    int propertyColumn = metaData
      .getPropertyColumn(propertySelector.getVariable(), propertySelector.getPropertyName());

    if (propertyColumn >= 0) {
      return embedding.getProperty(propertyColumn);
    } else {
      throw new MissingPropertyException(propertySelector.getPropertyName());
    }
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return variable.equals(propertySelector.getVariable()) ?
      Sets.newHashSet(propertySelector.getPropertyName()) : new HashSet<>(0);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PropertySelectorComparable that = (PropertySelectorComparable) o;

    return propertySelector != null ?
      propertySelector.equals(that.propertySelector) :
      that.propertySelector == null;
  }

  @Override
  public int hashCode() {
    return propertySelector != null ? propertySelector.hashCode() : 0;
  }
}
