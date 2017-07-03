
package org.gradoop.flink.model.impl.operators.matching.common.query;

/**
 * A {@link Traverser} that can start at a specified query vertex.
 */
public interface RootedTraverser extends Traverser {

  /**
   * Traverse the query graph starting at the given vertex.
   *
   * @param rootVertex start vertex
   * @return traversal code
   */
  TraversalCode traverse(long rootVertex);
}
