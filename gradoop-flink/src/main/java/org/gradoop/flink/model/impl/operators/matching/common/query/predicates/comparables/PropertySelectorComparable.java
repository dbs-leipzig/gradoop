/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
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
   * Returns the query variable of the property selector.
   *
   * @return query variable
   */
  public String getVariable() {
    return this.propertySelector.getVariable();
  }

  /**
   * Returns the property key of the property selector.
   *
   * @return property key
   */
  public String getPropertyKey() {
    return this.propertySelector.getPropertyName();
  }

  /**
   * Returns the specified property as property value
   *
   * @param embedding the embedding holding the data
   * @param metaData meta data describing the embedding
   * @return the property value
   */
  @Override
  public PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    int propertyColumn = metaData
      .getPropertyColumn(propertySelector.getVariable(), propertySelector.getPropertyName());

    return embedding.getProperty(propertyColumn);
  }

  @Override
  public PropertyValue evaluate(GraphElement element) {
    if (propertySelector.getPropertyName().equals("__label__")) {
      return PropertyValue.create(element.getLabel());
    }

    return element.hasProperty(propertySelector.getPropertyName()) ? element.getPropertyValue
      (propertySelector.getPropertyName()) : PropertyValue.NULL_VALUE;
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
