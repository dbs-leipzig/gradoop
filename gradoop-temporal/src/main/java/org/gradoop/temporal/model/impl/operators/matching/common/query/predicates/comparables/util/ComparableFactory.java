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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.util;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.ElementSelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.DurationComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MaxTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.MinTimePointComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeConstantComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeLiteralComparable;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TimeSelectorComparable;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeConstant;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

/**
 * Class for creating a {@link QueryComparable} wrapper for a GDL {@link ComparableExpression}
 */
public class ComparableFactory {

  /**
   * Create a wrapper for a GDL comparable
   *
   * @param expression the GDL element to wrap
   * @return wrapper for expression
   * @throws IllegalArgumentException if expression is no GDL ComparableExpression
   */
  public static QueryComparable createComparableFrom(ComparableExpression expression) {
    if (expression.getClass() == Literal.class) {
      return new LiteralComparable((Literal) expression);
    } else if (expression.getClass() == PropertySelector.class) {
      return new PropertySelectorComparable((PropertySelector) expression);
    } else if (expression.getClass() == ElementSelector.class) {
      return new ElementSelectorComparable((ElementSelector) expression);
    } else if (expression.getClass() == TimeLiteral.class) {
      return new TimeLiteralComparable((TimeLiteral) expression);
    } else if (expression.getClass() == TimeSelector.class) {
      return new TimeSelectorComparable((TimeSelector) expression);
    } else if (expression.getClass() == MinTimePoint.class) {
      return new MinTimePointComparable((MinTimePoint) expression);
    } else if (expression.getClass() == MaxTimePoint.class) {
      return new MaxTimePointComparable((MaxTimePoint) expression);
    } else if (expression.getClass() == TimeConstant.class) {
      return new TimeConstantComparable((TimeConstant) expression);
    } else if (expression.getClass() == Duration.class) {
      return new DurationComparable((Duration) expression);
    } else {
      throw new IllegalArgumentException(
        expression.getClass() + " is not a GDL ComparableExpression"
      );
    }
  }
}
