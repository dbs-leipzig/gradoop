
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;

/**
 * Filters vertices by a given predicate
 */
public class FilterVertex implements FilterFunction<Vertex> {

  /**
   * Filter predicate
   */
  private final CNF predicates;

  /**
   * Creates a new UDF
   *
   * @param predicates filter predicates
   */
  public FilterVertex(CNF predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean filter(Vertex vertex) throws Exception {
    return predicates.evaluate(vertex);
  }
}
