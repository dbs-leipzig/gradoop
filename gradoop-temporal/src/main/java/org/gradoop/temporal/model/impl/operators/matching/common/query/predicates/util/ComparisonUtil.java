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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;

/**
 * Utility methods for handling objects of type {@link ComparisonExpression}
 */
public class ComparisonUtil {

  /**
   * Checks whether a comparison expression compares temporal elements
   * @param comparison comparison expression to check
   * @return true iff expression wraps a well-formed temporal comparison
   */
  public static boolean isTemporal(ComparisonExpression comparison) {
    return comparison.getLhs() instanceof TemporalComparable &&
      comparison.getRhs() instanceof TemporalComparable;
  }
}
