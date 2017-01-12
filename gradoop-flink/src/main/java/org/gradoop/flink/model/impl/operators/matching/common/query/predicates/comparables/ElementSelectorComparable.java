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

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.exceptions.MissingElementException;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.ElementSelector;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps an {@link org.s1ck.gdl.model.comparables.ElementSelector}
 */
public class ElementSelectorComparable extends QueryComparable {
  /**
   * Holds the wrapped ElementSelector
   */
  private final ElementSelector elementSelector;

  /**
   * Creates a new Wrapper
   *
   * @param elementSelector the wrapped ElementSelectorComparable
   */
  public ElementSelectorComparable(ElementSelector elementSelector) {
    this.elementSelector = elementSelector;
  }

  /**
   * Returns a property values that wraps the elements id
   *
   * @param embedding the embedding holding the data
   * @param metaData meta data describing the embedding
   * @return property value of element id
   * @throws MissingElementException if element is not specified in values mapping
   */
  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    int column = metaData.getColumn(elementSelector.getVariable());

    if (column == -1) {
      throw new MissingElementException(elementSelector.getVariable());
    }

    return PropertyValue.create(embedding.getId(column));
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return new HashSet<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ElementSelectorComparable that = (ElementSelectorComparable) o;

    return elementSelector != null ? elementSelector.equals(that.elementSelector) :
      that.elementSelector == null;

  }

  @Override
  public int hashCode() {
    return elementSelector != null ? elementSelector.hashCode() : 0;
  }
}
