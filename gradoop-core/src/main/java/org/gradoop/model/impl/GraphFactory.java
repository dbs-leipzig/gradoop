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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import org.gradoop.GConstants;
import org.gradoop.model.Graph;

import java.util.Map;

/**
 * Factory for creating graphs.
 */
public class GraphFactory {

  /**
   * Avoid instantiation.
   */
  public GraphFactory() {
  }

  /**
   * Creates a graph using the given id.
   *
   * @param id graph identifier
   * @return graph with identifier
   */
  public static Graph createDefaultGraphWithID(final Long id) {
    return createDefaultGraph(id, null, null, null);
  }

  /**
   * Creates a graph using the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph properties
   * @param vertices   vertices contained in the graph
   * @return graph
   */
  public static Graph createDefaultGraph(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Long> vertices) {
    checkGraphID(id);
    if (label == null || "".equals(label)) {
      return new DefaultGraph(id, GConstants.DEFAULT_GRAPH_LABEL, properties,
        vertices);
    } else {
      return new DefaultGraph(id, label, properties, vertices);
    }

  }

  /**
   * Checks if the given graphID is valid.
   *
   * @param graphID graph identifier
   */
  private static void checkGraphID(final Long graphID) {
    if (graphID == null) {
      throw new IllegalArgumentException("graphID must not be null");
    }
  }
}
