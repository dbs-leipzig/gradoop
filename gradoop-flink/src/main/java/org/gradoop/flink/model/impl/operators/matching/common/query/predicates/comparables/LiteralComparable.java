/*
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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
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
   * Returns the value of the literal.
   *
   * @return literal value
   */
  public Object getValue() {
    return literal.getValue();
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
  public PropertyValue evaluate(GraphElement element) {
    return PropertyValue.create(literal.getValue());
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    return new HashSet<>(0);
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
