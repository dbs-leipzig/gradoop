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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.util;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.QueryPredicateTPGM;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.AndPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.NotPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.OrPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.booleans.XorPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.booleans.Not;
import org.s1ck.gdl.model.predicates.booleans.Or;
import org.s1ck.gdl.model.predicates.booleans.Xor;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

/**
 * Class for creating a {@link QueryPredicate} wrapper for a GDL {@link Predicate}
 */
public class QueryPredicateFactory {

  /**
   * Create a wrapper for a GDL comparable
   *
   * @param predicate the GDL predicate to wrap
   * @return wrapper for predicate
   * @throws IllegalArgumentException if predicate is no GDL Predicate
   */
  public static QueryPredicateTPGM createFrom(Predicate predicate) {
    if (predicate.getClass() == And.class) {
      return new AndPredicate((And) predicate);

    } else if (predicate.getClass() == Or.class) {
      return new OrPredicate((Or) predicate);

    } else if (predicate.getClass() == Xor.class) {
      return new XorPredicate((Xor) predicate);

    } else if (predicate.getClass() == Not.class) {
      return new NotPredicate((Not) predicate);

    } else if (predicate.getClass() == Comparison.class) {
      return new ComparisonExpressionTPGM((Comparison) predicate);
    } else {
      throw new IllegalArgumentException(predicate.getClass() + " is not a GDL Predicate");
    }
  }

}
