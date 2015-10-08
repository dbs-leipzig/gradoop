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

package org.gradoop.model.impl.pojo;

import org.gradoop.model.api.GraphData;

import java.util.Map;

/**
 * Transient representation of a graph.
 */
public class DefaultGraphData extends DefaultElement implements GraphData {

  /**
   * Default constructor.
   */
  public DefaultGraphData() {
  }

  /**
   * Creates a graph based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      labels of that graph
   * @param properties key-value-map
   */
  DefaultGraphData(final Long id, final String label,
    final Map<String, Object> properties) {
    super(id, label, properties);
  }

  @Override
  public String toString() {
    return "DefaultGraphData{" +
      super.toString() +
      '}';
  }
}
