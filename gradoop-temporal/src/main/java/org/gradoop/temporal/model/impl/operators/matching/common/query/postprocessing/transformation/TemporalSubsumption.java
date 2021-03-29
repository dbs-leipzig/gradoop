/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.gdl.model.comparables.time.TimeLiteral;
import org.gradoop.gdl.model.comparables.time.TimeSelector;
import org.gradoop.gdl.utils.Comparator;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;

/**
 * Uses temporal information to subsume constraints that compare a time selector to a time literal.
 * Here, a temporal comparison c1 subsumes a temporal comparison c2 iff
 * c1 logically implies c2 and c1 is not equal to c2.
 * !!! This class assumes the input to be normalized, i.e. not to contain > or >= !!!
 */
public class TemporalSubsumption extends Subsumption {
  @Override
  public boolean subsumes(ComparisonExpression c1, ComparisonExpression c2) {
    // check if comparisons are both of the desired form and could thus be - potentially - subsumed
    if (!structureMatches(c1, c2)) {
      return false;
    }
    // ensure that selectors in both comparisons are always on the left hand side
    boolean selectorIsLeft = c1.getLhs() instanceof TimeSelectorComparable;
    if (!selectorIsLeft) {
      c1 = c1.switchSides();
      c2 = c2.switchSides();
    }
    // check if both comparisons' variables are equal (otherwise, no subsumption is possible)
    String var1 = ((TimeSelectorComparable) (c1.getLhs())).getWrappedComparable().getVariable();
    String var2 = ((TimeSelectorComparable) (c2.getLhs())).getWrappedComparable().getVariable();
    if (!var1.equals(var2)) {
      return false;
    }
    // check if both comparisons' time properties (tx_from, tx_to,...) are equal
    // (otherwise, no subsumption is possible)
    TimeSelector.TimeField field1 = ((TimeSelector) ((TimeSelectorComparable)
      (c1.getLhs())).getWrappedComparable()).getTimeProp();
    TimeSelector.TimeField field2 = ((TimeSelector) ((TimeSelectorComparable) (c2.getLhs()))
      .getWrappedComparable()).getTimeProp();
    if (!field1.equals(field2)) {
      return false;
    }
    // unwrap the rest of the comparisons and check whether c1 implies c2
    Comparator comparator1 = c1.getComparator();
    Comparator comparator2 = c2.getComparator();
    Long literal1 = ((TimeLiteral) ((TimeLiteralComparable) c1.getRhs()).getWrappedComparable())
      .getMilliseconds();
    Long literal2 = ((TimeLiteral) ((TimeLiteralComparable) c2.getRhs()).getWrappedComparable())
      .getMilliseconds();
    return implies(comparator1, literal1, comparator2, literal2) && !c1.equals(c2);
  }

  /**
   * Checks if a comparison constraint (a comparator1 literal1) implies a comparison
   * constraint (a comparator2 literal2).
   *
   * @param comparator1 comparator of the potentially implying constraint
   * @param literal1    rhs literal of the potentially implying constraint
   * @param comparator2 comparator of the potentially implied constraint
   * @param literal2    rhs literal of the potentially implied constraint
   * @return true iff (a comparator1 literal1) implies (a comparator2 literal2)
   */
  private boolean implies(Comparator comparator1, Long literal1, Comparator comparator2, Long literal2) {
    switch (comparator1) {
    case EQ:
      switch (comparator2) {
      case EQ: return literal1.equals(literal2);
      case NEQ: return !literal1.equals(literal2);
      case LTE: return literal1 <= literal2;
      case LT: return literal1 < literal2;
      case GTE: return literal1 >= literal2;
      case GT: return literal1 > literal2;
      default: return false;
      }
    case LTE:
      switch (comparator2) {
      case NEQ: case LT: return literal1 < literal2;
      case LTE: return literal1 <= literal2;
      default: return false;
      }
    case LT:
      switch (comparator2) {
      case NEQ: case LT: return literal1 <= literal2;
      case LTE: return literal1 - 1 <= literal2;
      default: return false;
      }
    case GTE:
      switch (comparator2) {
      case NEQ: case GT: return literal1 > literal2;
      case GTE: return literal1 >= literal2;
      default: return false;
      }
    case GT:
      switch (comparator2) {
      case NEQ: case GT: return literal1 >= literal2;
      case GTE: return literal1 + 1 >= literal2;
      default: return false;
      }
    default: return false;
    }
  }

  /**
   * Checks if two comparisons "match" for a subsumption.
   * Here, only comparisons comparing a selector to a literal are relevant.
   * As the CNF is assumed to be normalized (no <,<=), comparisons c1 and
   * c2 are relevant iff they both have the form (selector comparator literal)
   * or both have the form (literal comparator selector)
   *
   * @param c1 comparison
   * @param c2 comparison
   * @return true iff the structures of c1 and c2 match
   * according to the criteria defined above
   */
  private boolean structureMatches(ComparisonExpression c1, ComparisonExpression c2) {
    return (c1.getLhs() instanceof TimeSelectorComparable &&
      c2.getLhs() instanceof TimeSelectorComparable &&
      c1.getRhs() instanceof TimeLiteralComparable &&
      c2.getRhs() instanceof TimeLiteralComparable) ||
      (c1.getLhs() instanceof TimeLiteralComparable &&
        c2.getLhs() instanceof TimeLiteralComparable &&
        c1.getRhs() instanceof TimeSelectorComparable &&
        c2.getRhs() instanceof TimeSelectorComparable);
  }
}
