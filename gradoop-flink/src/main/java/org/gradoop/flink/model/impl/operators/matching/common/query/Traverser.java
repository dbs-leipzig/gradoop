
package org.gradoop.flink.model.impl.operators.matching.common.query;

/**
 * Used to traverse a query graph.
 */
public interface Traverser {

  /**
   * Traverse the graph.
   *
   * @return traversal code
   */
  TraversalCode traverse();

  /**
   * Set the query handler to access the query graph.
   *
   * @param queryHandler query handler
   */
  void setQueryHandler(QueryHandler queryHandler);

  /**
   * Returns the query handler.
   *
   * @return query handler
   */
  QueryHandler getQueryHandler();

}
