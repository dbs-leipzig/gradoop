
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

/**
 * Filters an Edge by a given predicate
 */
public class FilterEdge implements FilterFunction<Edge> {

  /**
   * Filter predicate
   */
  private final CNF predicates;

  /**
   * Creates a new UDF
   *
   * @param predicates filter predicates
   */
  public FilterEdge(CNF predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean filter(Edge edge) throws Exception {
    return predicates.evaluate(edge);
  }
}
