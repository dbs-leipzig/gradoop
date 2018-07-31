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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.AndPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.NotPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.OrPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.booleans.XorPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
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
