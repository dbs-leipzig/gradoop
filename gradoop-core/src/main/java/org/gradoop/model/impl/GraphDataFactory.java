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
import org.gradoop.model.GraphData;

import java.util.Map;
import java.util.Set;

/**
 * Factory for creating graphs.
 */
public class GraphDataFactory {

  /**
   * Avoid instantiation.
   */
  public GraphDataFactory() {
  }

  /**
   * Creates a graph using the given id.
   *
   * @param id graph identifier
   * @return graph with identifier
   */
  public static GraphData createDefaultGraphWithID(final Long id) {
    return createDefaultGraph(id, GConstants.DEFAULT_GRAPH_LABEL, null, null,
      null);
  }

  /**
   * Creates a graph using the given id.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph with identifier and label
   */
  public static GraphData createDefaultGraphWithIDAndLabel(final Long id,
    final String label) {
    return createDefaultGraph(id, label, null, null, null);
  }

  /**
   * Creates a graph using the given id.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph with identifier and label
   */
  public static GraphData createDefaultGraphWithIDAndLabelAndProperties(
    final Long id, final String label, Map<String, Object> properties) {
    return createDefaultGraph(id, label, properties, null, null);
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
  public static GraphData createDefaultGraph(final Long id, final String label,
    final Map<String, Object> properties, final Set<Long> vertices,
    final Set<Long> edges) {
    checkGraphID(id);
    if (label == null || "".equals(label)) {
      return new DefaultGraphData(id, GConstants.DEFAULT_GRAPH_LABEL,
        properties, vertices, edges);
    } else {
      return new DefaultGraphData(id, label, properties, vertices, edges);
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
