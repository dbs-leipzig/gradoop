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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a collection disjunct predicates
 * This can be used to represent a CNF
 */
public class CNFElement extends PredicateCollection<ComparisonExpression> {

  /**
   * Creates a new CNFElement with empty predicate list
   */
  public CNFElement() {
    this.predicates = new ArrayList<>();
  }

  /**
   * Creats a new CNFElement with preset predicate list
   *
   * @param predicates predicates
   */
  public CNFElement(List<ComparisonExpression> predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    for (ComparisonExpression comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(embedding, metaData)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean evaluate(GraphElement element) {
    for (ComparisonExpression comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(element)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (ComparisonExpression comparisonExpression : predicates) {
      variables.addAll(comparisonExpression.getVariables());
    }
    return variables;
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();
    for (ComparisonExpression comparisonExpression : predicates) {
      properties.addAll(comparisonExpression.getPropertyKeys(variable));
    }
    return properties;
  }

  @Override
  public String operatorName() {
    return "OR";
  }
}
