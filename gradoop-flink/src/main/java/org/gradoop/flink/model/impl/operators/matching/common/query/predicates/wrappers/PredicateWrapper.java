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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans
  .AndWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans
  .NotWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans
  .OrWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans
  .XorWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .expressions.ComparisonWrapper;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

/**
 * Wrapps a Predicate
 */
public abstract class PredicateWrapper {

  /**
   * Generic wrapper function to wrap a given predicate
   * @param predicate the predicate to be warpped
   * @return the wrapped predicate
   */
  public static PredicateWrapper wrap(Predicate predicate) {
    if (predicate.getClass() == And.class) {
      return new AndWrapper((And) predicate);

    } else if (predicate.getClass() == Or.class) {
      return new OrWrapper((Or) predicate);

    } else if (predicate.getClass() == Xor.class) {
      return new XorWrapper((Xor) predicate);

    } else if (predicate.getClass() == Not.class) {
      return new NotWrapper((Not) predicate);

    } else if (predicate.getClass() == Comparison.class) {
      return new ComparisonWrapper((Comparison) predicate);
    }

    return null;
  }

  /**
   * Converts the predicate into Conjunctive Normal Form
   * @return the predicate in CNF
   */
  public abstract CNF asCNF();
}
