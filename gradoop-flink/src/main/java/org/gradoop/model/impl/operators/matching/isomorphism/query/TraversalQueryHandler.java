package org.gradoop.model.impl.operators.matching.isomorphism.query;

import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;

/**
 * Wraps a QueryHandler to add traversal functionality.
 */
public class TraversalQueryHandler extends QueryHandler {
  Traversal traversal;

  /**
   * Creates a new traversal query handler.
   *
   * @param gdlString GDL query string
   */
  private TraversalQueryHandler(String gdlString) {
    super(gdlString);
    traversal = null;
  }

  /**
   * Create a traversal query handler using the given GQL query string.
   * @param gdlString gld query string
   * @return new TraversalQueryHandler
   */
  public static TraversalQueryHandler fromString(String gdlString) {
    return new TraversalQueryHandler(gdlString);
  }

  /**
   * Return the traversal. If it has not been created yet, create one first.
   * Default is depth-first traversal.
   * @return traversal
   */
  public Traversal getTraversal() {
    if (traversal == null) {
      traversal =
        DFSTraversal.createDFSTraversal(this.getVertices(), this.getEdges());
    }
    return this.traversal;
  }
}
