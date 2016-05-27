/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.matching.isomorphism.query;

import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;

/**
 * Wraps a QueryHandler to add traversal functionality.
 */
public class TraversalQueryHandler extends QueryHandler {
  /**
   * Constructed traversal through the graph.
   */
  private Traversal traversal;

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
