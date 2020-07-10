/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;


import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a collection disjunct predicates
 * This can be used to represent a CNF
 */
public class CNFElementTPGM extends TemporalPredicateCollection<ComparisonExpressionTPGM> {

  /**
   * Creates a new CNFElement with empty predicate list
   */
  public CNFElementTPGM() {
    this.predicates = new ArrayList<>();
  }

  /**
   * Creates a new CNFElement with preset predicate list
   *
   * @param predicates predicates
   */
  public CNFElementTPGM(List<ComparisonExpressionTPGM> predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    for (ComparisonExpressionTPGM comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(embedding, metaData)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean evaluate(GraphElement element) {
    for (ComparisonExpressionTPGM comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(element)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (ComparisonExpressionTPGM comparisonExpression : predicates) {
      variables.addAll(comparisonExpression.getVariables());
    }
    return variables;
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();
    for (ComparisonExpressionTPGM comparisonExpression : predicates) {
      properties.addAll(comparisonExpression.getPropertyKeys(variable));
    }
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof CNFElementTPGM)) {
      return false;
    } else {
      CNFElementTPGM other = (CNFElementTPGM) o;
      if (getPredicates().size() != other.getPredicates().size()) {
        return false;
      }
      for (QueryPredicateTPGM pred1 : getPredicates()) {
        boolean foundMatch = false;
        for (QueryPredicateTPGM pred2 : ((CNFElementTPGM) o).getPredicates()) {
          if (pred1.equals(pred2)) {
            foundMatch = true;
            break;
          }
        }
        if (!foundMatch) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode(){
    int sum = 7;
    for(ComparisonExpressionTPGM pred: getPredicates()){
      sum += 31 * sum + pred.hashCode();
    }
    return sum;
  }

  @Override
  public String operatorName() {
    return "OR";
  }
}

