package org.gradoop.model.operators;

import org.apache.hadoop.io.Writable;
import org.gradoop.model.Vertex;

/**
 * Used to calculate a double aggregate based on a single vertex.
 */
public interface VertexAggregate {
  /**
   * Returns a value based on the given vertex.
   *
   * @param vertex vertex to be aggregated
   * @return aggregate
   */
  Writable aggregate(Vertex vertex);
}
