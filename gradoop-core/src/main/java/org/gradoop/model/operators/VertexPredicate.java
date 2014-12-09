package org.gradoop.model.operators;

import org.gradoop.model.Vertex;

/**
 * Evaluates to true if the given vertex satisfies the predicate function.
 * This can be used in MapReduce jobs.
 */
public interface VertexPredicate {
  /**
   * Returns true if the given vertex fulfils the predicate function.
   *
   * @param vertex vertex to evaluate
   * @return true, if vertex fulfils predicate, else false
   */
  boolean evaluate(Vertex vertex);
}
