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

package org.gradoop.storage.impl.hbase;

import com.google.common.collect.Sets;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.storage.api.PersistentGraphHead;

import java.util.Set;

/**
 * Represents a persistent vertex data object.
 */
public class HBaseGraphHead extends HBaseElement<EPGMGraphHead> implements PersistentGraphHead {

  /**
   * Vertex identifiers contained in that logical graph.
   */
  private Set<Long> vertices;

  /**
   * Edge identifiers contained in that logical graph.
   */
  private Set<Long> edges;

  /**
   * Default constructor.
   */
  public HBaseGraphHead() {
  }

  /**
   * Creates  persistent graph data.
   *
   * @param graphHead encapsulated graph data
   * @param vertices  vertices contained in that graph
   * @param edges     edges contained in that graph
   */
  public HBaseGraphHead(EPGMGraphHead graphHead, Set<Long> vertices,
    Set<Long> edges) {
    super(graphHead);
    this.vertices = vertices;
    this.edges = edges;
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

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HBaseGraphHead{");
    sb.append("super=").append(super.toString());
    sb.append(", vertices=").append(vertices);
    sb.append(", edges=").append(edges);
    sb.append('}');
    return sb.toString();
  }
}
