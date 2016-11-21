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

package org.gradoop.flink.representation.transactional.traversalcode;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;

/**
 * Mapping between an embedding and a DFS code.
 */
public class TraversalEmbedding {

  /**
   * Initial vertex discovery times.
   */
  private final List<GradoopId> vertexIds;

  /**
   * Included edges.
   */
  private final List<GradoopId> edgeIds;

  /**
   * Constructor.
   *
   * @param vertexIds vertex ids
   * @param edgeIds edge ids
   */
  public TraversalEmbedding(List<GradoopId> vertexIds, List<GradoopId> edgeIds) {
    this.vertexIds = vertexIds;
    this.edgeIds = edgeIds;
  }

  /**
   * Default constructor.
   */
  public TraversalEmbedding() {
    this.vertexIds = Lists.newArrayList();
    this.edgeIds = Lists.newArrayList();
  }

  /**
   * Constructor.
   *
   * @param parent parent embedding
   */
  public TraversalEmbedding(TraversalEmbedding parent) {
    this.vertexIds = Lists.newArrayList(parent.getVertexIds());
    this.edgeIds = Lists.newArrayList(parent.getEdgeIds());
  }

  public List<GradoopId> getEdgeIds() {
    return edgeIds;
  }

  public List<GradoopId> getVertexIds() {
    return vertexIds;
  }

  @Override
  public String toString() {
    return vertexIds + ";" + edgeIds;
  }
}
