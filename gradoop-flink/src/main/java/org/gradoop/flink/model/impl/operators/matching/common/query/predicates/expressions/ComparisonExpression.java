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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
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
