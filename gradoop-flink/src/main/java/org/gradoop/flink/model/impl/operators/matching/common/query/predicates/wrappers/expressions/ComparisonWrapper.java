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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.expressions;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .ComparableWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.PredicateWrapper;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.Map;
import java.util.Set;

/**
 * Wraps a {@link Comparison}
 */
public class ComparisonWrapper extends PredicateWrapper {
  /**
   * Holds the wrapped comparison
   */
  private final Comparison comparison;

  /**
   * Creates a new comparison wrapped
   * @param comparison the wrapped comparison
   */
  public ComparisonWrapper(Comparison comparison) {
    this.comparison = comparison;
  }

  /**
   * Returns the wrapped left hand side of the comparison
   * @return wrapped left hand side
   */
  public ComparableWrapper getLhs() {
    return ComparableWrapper.wrap(comparison.getComparableExpressions()[0]);
  }

  /**
   * Returns the wrapped left hand side of the comparison
   * @return wrapped left hand side
   */
  public ComparableWrapper getRhs() {
    return ComparableWrapper.wrap(comparison.getComparableExpressions()[1]);
  }

  /**
   * Evaluates the comparison with respect to the given data
   * @param values mapping of variables to embedding entries
   * @return result of the evaluation
   */
  public boolean evaluate(Map<String, EmbeddingEntry> values) {
    PropertyValue lhsValue = getLhs().evaluate(values);
    PropertyValue rhsValue = getRhs().evaluate(values);

    try {
      int result = lhsValue.compareTo(rhsValue);

      return
        comparison.getComparator() == Comparator.EQ  && result ==  0 ||
        comparison.getComparator() == Comparator.NEQ && result !=  0 ||
        comparison.getComparator() == Comparator.LT  && result == -1 ||
        comparison.getComparator() == Comparator.GT  && result ==  1 ||
        comparison.getComparator() == Comparator.LTE && result <=  0 ||
        comparison.getComparator() == Comparator.GTE && result >=  0;

    } catch (IllegalArgumentException e) {
      return comparison.getComparator() == Comparator.NEQ;
    }
  }

  /**
   * Returns the variables referenced by the expression
   * @return set of variables
   */
  public Set<String> getVariables() {
    return comparison.getVariables();
  }

  /**
   * Converts the predicate into conjunctive normal form
   * @return predicate in cnf
   */
  @Override
  public CNF asCNF() {
    CNF cnf = new CNF();
    CNFElement cnfElement = new CNFElement();

    cnfElement.addPredicate(this);
    cnf.addPredicate(cnfElement);

    return cnf;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ComparisonWrapper that = (ComparisonWrapper) o;

    return comparison != null ? comparison.equals(that.comparison) : that.comparison == null;

  }

  @Override
  public int hashCode() {
    return comparison != null ? comparison.hashCode() : 0;
  }

  @Override
  public String toString() {
    return comparison.toString();
  }
}
