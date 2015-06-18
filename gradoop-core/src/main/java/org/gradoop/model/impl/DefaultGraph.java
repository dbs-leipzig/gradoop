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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.gradoop.model.Graph;

import java.util.List;
import java.util.Map;

/**
 * Transient representation of a graph.
 */
public class DefaultGraph extends LabeledPropertyContainer implements Graph {

  /**
   * Holds vertex identifiers contained in that graph.
   */
  private List<Long> vertices;

  /**
   * Creates a graph based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      labels of that graph
   * @param properties key-value-map
   * @param vertices   vertices contained in that graph
   */
  DefaultGraph(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Long> vertices) {
    super(id, label, properties);
    this.vertices = (vertices != null) ? Lists.newArrayList(vertices) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addVertex(Long vertexID) {
    if (vertices != null) {
      vertices.add(vertexID);
    } else {
      vertices = Lists.newArrayList(vertexID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Long> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getVertexCount() {
    return (vertices != null) ? Iterables.size(vertices) : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "DefaultGraph{" +
      "id=" + getID() +
      ", label=" + getLabel() +
      ", vertices=" + vertices +
      '}';
  }
}
