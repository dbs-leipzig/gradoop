package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparableFactory;
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

import java.io.Serializable;

/**
 * Factory for temporal comparables
 */
public class ComparableTPGMFactory extends QueryComparableFactory {

  /**
   * Holds the system time stamp when the query was issued
   */
  TimeLiteral now;

  /**
   * Create a new instance
   * @param now system time of the query
   */
  public ComparableTPGMFactory(TimeLiteral now){
    this.now = now;
  }

  /**
   * Create a new instance
   */
  public ComparableTPGMFactory(){
    this(new TimeLiteral("now"));
  }

  @Override
  public QueryComparable createFrom(ComparableExpression expression) {
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
      return new DurationComparable((Duration) expression, now);
    } else {
      throw new IllegalArgumentException(
        expression.getClass() + " is not a temporal GDL ComparableExpression"
      );
    }
  }
}
