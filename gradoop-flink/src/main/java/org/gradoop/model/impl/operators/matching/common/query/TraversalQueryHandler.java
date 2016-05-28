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

package org.gradoop.model.impl.operators.matching.common.query;

/**
 * Extends a {@link QueryHandler} with traversal functionality.
 */
public class TraversalQueryHandler extends QueryHandler {
  /**
   * Used to construct a {@link TraversalCode}
   */
  private Traverser traverser;

  /**
   * Creates a new traversing query handler.
   *
   * @param gdlString GDL query string
   */
  public TraversalQueryHandler(String gdlString) {
    super(gdlString);
    traverser = new DFSTraverser(this);
  }

  /**
   * Creates a new traversing query handler.
   *
   * @param gdlString     GDL query string
   * @param traverser     Graph traverser
   */
  public TraversalQueryHandler(String gdlString, Traverser traverser) {
    super(gdlString);
    this.traverser = traverser;
  }

  public TraversalCode getTraversalCode() {
    return traverser.traverse();
  }
}
