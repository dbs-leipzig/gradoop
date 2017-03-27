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

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.Set;

/**
 * Wraps a {@link Comparison}
 */
public class ComparisonExpression extends QueryPredicate {
  /**
   * Holds the wrapped comparison
   */
  private final Comparison comparison;

  /**
   * Creates a new comparison wrapped
   * @param comparison the wrapped comparison
   */
  public ComparisonExpression(Comparison comparison) {
    this.comparison = comparison;
  }

  /**
   * Returns the wrapped left hand side of the comparison
   * @return wrapped left hand side
   */
  public QueryComparable getLhs() {
    return QueryComparable.createFrom(comparison.getComparableExpressions()[0]);
  }

  /**
   * Returns the wrapped left hand side of the comparison
   * @return wrapped left hand side
   */
  public QueryComparable getRhs() {
    return QueryComparable.createFrom(comparison.getComparableExpressions()[1]);
  }

  public Comparator getComparator() {
    return comparison.getComparator();
  }

  /**
   * Evaluates the comparisson for the given embedding
   *
   * @param embedding the embedding record holding the data
   * @param metaData the embedding meta data
   * @return evaluation result
   */
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    PropertyValue lhsValue = getLhs().evaluate(embedding, metaData);
    PropertyValue rhsValue = getRhs().evaluate(embedding, metaData);

    return compare(lhsValue, rhsValue);
  }

  /**
   * Evaluates the comparison for the given graph element
   *
   * @param element GraphElement under which the comparison will be evaluated
   * @return evaluation result
   */
  public boolean evaluate(GraphElement element) {
    PropertyValue lhsValue = getLhs().evaluate(element);
    PropertyValue rhsValue = getRhs().evaluate(element);

    return compare(lhsValue, rhsValue);
  }

  /**
   * Compares two property values with the given comparator
   *
   * @param lhsValue left property value
   * @param rhsValue right property value
   * @return comparison result
   */
  private boolean compare(PropertyValue lhsValue, PropertyValue rhsValue) {
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
   * Returns the properties referenced by the expression for a given variable
   * @param variable the variable
   * @return set of referenced properties
   */
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = getLhs().getPropertyKeys(variable);
    properties.addAll(getRhs().getPropertyKeys(variable));

    return properties;
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

    ComparisonExpression that = (ComparisonExpression) o;

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
