package org.gradoop.model.operators;

import org.gradoop.model.Vertex;

/**
 * Used to calculate an integer aggregate based on a single vertex.
 */
public interface VertexIntegerAggregate {
  /**
   * Returns a value of type {@link Integer} based on the given vertex.
   *
   * @param vertex vertex to be aggregated
   * @return aggregate
   */
  Integer aggregate(Vertex vertex);
}
