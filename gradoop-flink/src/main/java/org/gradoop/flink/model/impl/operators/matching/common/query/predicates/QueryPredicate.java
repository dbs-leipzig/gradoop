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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans
  .AndPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans
  .NotPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.OrPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans
  .XorPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions
  .ComparisonExpression;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.io.Serializable;

/**
 * Wrapps a Predicate
 */
public abstract class QueryPredicate implements Serializable {

  /**
   * Generic wrapper function to createFrom a given predicate
   * @param predicate the predicate to be warpped
   * @return the wrapped predicate
   */
  public static QueryPredicate createFrom(Predicate predicate) {
    if (predicate.getClass() == And.class) {
      return new AndPredicate((And) predicate);

    } else if (predicate.getClass() == Or.class) {
      return new OrPredicate((Or) predicate);

    } else if (predicate.getClass() == Xor.class) {
      return new XorPredicate((Xor) predicate);

    } else if (predicate.getClass() == Not.class) {
      return new NotPredicate((Not) predicate);

    } else if (predicate.getClass() == Comparison.class) {
      return new ComparisonExpression((Comparison) predicate);
    } else {
      throw new IllegalArgumentException(predicate.getClass() + " is not a GDL Predicate");
    }
  }

  /**
   * Converts the predicate into Conjunctive Normal Form
   * @return the predicate in CNF
   */
  public abstract CNF asCNF();
}
