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

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;

import java.util.Map;

/**
 * Default transient representation of edge data that is a POJO and can thus be
 * used with Apache Flink.
 */
public class EdgePojo extends GraphElementPojo implements EPGMEdge {

  /**
   * Vertex identifier of the source vertex.
   */
  private GradoopId sourceVertexId;

  /**
   * Vertex identifier of the target vertex.
   */
  private GradoopId targetVertexId;

  /**
   * Default constructor is necessary to apply to POJO rules.
   */
  public EdgePojo() {
  }

  /**
   * Creates an edge instance based on the given parameters.
   *  @param id             the unique id of the edge
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds         graphs that edge is contained in
   */
  EdgePojo(
    final GradoopId id,
    final String label,
    final GradoopId sourceVertexId,
    final GradoopId targetVertexId,
    final Map<String, Object> properties,
    GradoopIds graphIds) {

    super(id, label, properties, graphIds);
    this.sourceVertexId = sourceVertexId;
    this.targetVertexId = targetVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getSourceVertexId() {
    return sourceVertexId;
  }

  /**
   * {@inheritDoc}
   * @param sourceVertexId
   */
  @Override
  public void setSourceVertexId(GradoopId sourceVertexId) {
    this.sourceVertexId = sourceVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getTargetVertexId() {
    return targetVertexId;
  }

  /**
   * {@inheritDoc}
   * @param targetVertexId
   */
  @Override
  public void setTargetVertexId(GradoopId targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "EdgePojo{" +
      super.toString() +
      ", sourceVertexId=" + sourceVertexId +
      ", targetVertexId=" + targetVertexId +
      '}';
  }
}
