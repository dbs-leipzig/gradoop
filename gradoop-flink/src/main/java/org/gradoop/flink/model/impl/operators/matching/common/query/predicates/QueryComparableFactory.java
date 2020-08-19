package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.s1ck.gdl.model.comparables.ComparableExpression;

import java.io.Serializable;

/**
 * Factory for @link{QueryComparable}s in order to support integration of newer GDL versions
 * in gradoop-temporal
 */
public abstract class QueryComparableFactory implements Serializable {

  /**
   * Creates a {@link QueryComparable} from a GDL comparable expression
   * @return QueryComparable
   */
  public abstract QueryComparable createFrom(ComparableExpression comparable);
}
