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
   */
  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    int column = metaData.getEntryColumn(elementSelector.getVariable());
    return PropertyValue.create(embedding.getId(column));
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    return PropertyValue.create(element.getId());
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
