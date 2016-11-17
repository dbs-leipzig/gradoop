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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  public boolean evaluate(Map<String, EmbeddingEntry> values) {
    for (ComparisonExpression comparisonExpressionWrapper : predicates) {
      if (comparisonExpressionWrapper.evaluate(values)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a set of variables referenced by the predicates
   * @return set of variables
   */
  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (ComparisonExpression comparisonExpression : predicates) {
      variables.addAll(comparisonExpression.getVariables());
    }
    return variables;
  }

  @Override
  public String operatorName() {
    return "OR";
  }
}
