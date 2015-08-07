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

import com.google.common.collect.Sets;
import org.gradoop.model.GraphData;

import java.util.Map;
import java.util.Set;

/**
 * Transient representation of a graph.
 */
public class DefaultGraphData extends EPGMElement implements GraphData {

  /**
   * Holds vertex identifiers contained in that graph.
   */
  private Set<Long> vertices;

  /**
   * Holds edge identifiers contained in that graph.
   */
  private Set<Long> edges;

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
   * @param vertices   vertices contained in that graph
   * @param edges      edges contains in that graph
   */
  DefaultGraphData(final Long id, final String label,
    final Map<String, Object> properties, final Set<Long> vertices,
    final Set<Long> edges) {
    super(id, label, properties);
    this.vertices = (vertices != null) ? Sets.newHashSet(vertices) : null;
    this.edges = (edges != null) ? Sets.newHashSet(edges) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setVertices(Set<Long> vertices) {
    this.vertices = vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addVertex(Long vertexID) {
    if (vertices != null) {
      vertices.add(vertexID);
    } else {
      vertices = Sets.newHashSet(vertexID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() {
    return (vertices != null) ? vertices.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Long> getEdges() {
    return edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEdges(Set<Long> edges) {
    this.edges = edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addEdge(Long edgeID) {
    if (edges != null) {
      edges.add(edgeID);
    } else {
      edges = Sets.newHashSet(edgeID);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() {
    return (edges != null) ? edges.size() : 0;
  }

  @Override
  public String toString() {
    return "DefaultGraphData{" +
      super.toString() +
      ", vertices=" + vertices +
      ", edges=" + edges +
      '}';
  }
}
