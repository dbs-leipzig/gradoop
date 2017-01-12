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
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.Literal;

import java.util.HashSet;
import java.util.Set;

/**
 * Wraps a {@link org.s1ck.gdl.model.comparables.Literal}
 */
public class LiteralComparable extends QueryComparable {
  /**
   * Holds the wrapped literal
   */
  private final Literal literal;

  /**
   * Creates a new wrapper
   *
   * @param literal the wrapped literal
   */
  public LiteralComparable(Literal literal) {
    this.literal = literal;
  }

  /**
   * Returns a property value that wraps the represented literal
   *
   * @param embedding the embedding holding the data
   * @param metaData meta data describing the embedding
   * @return property value of literal value
   */
  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    return PropertyValue.create(literal.getValue());
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

    LiteralComparable that = (LiteralComparable) o;

    return literal != null ? literal.equals(that.literal) : that.literal == null;

  }

  @Override
  public int hashCode() {
    return literal != null ? literal.hashCode() : 0;
  }
}
