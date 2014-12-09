package org.gradoop.model.operators;

import org.gradoop.model.Vertex;

/**
 * Used to calculate a double aggregate based on a single vertex.
 */
public interface VertexDoubleAggregate {
  /**
   * Returns a value of type {@link Double} based on the given vertex.
   *
   * @param vertex vertex to be aggregated
   * @return aggregate
   */
  Double aggregate(Vertex vertex);
}
