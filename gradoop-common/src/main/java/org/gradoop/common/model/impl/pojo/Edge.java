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

package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;

/**
 * POJO Implementation of an EPGM edge.
 */
public class Edge extends GraphElement implements EPGMEdge {

  /**
   * EPGMVertex identifier of the source vertex.
   */
  private GradoopId sourceId;

  /**
   * EPGMVertex identifier of the target vertex.
   */
  private GradoopId targetId;

  /**
   * Default constructor is necessary to apply to POJO rules.
   */
  public Edge() {
  }

  /**
   * Creates an edge instance based on the given parameters.
   *
   * @param id          edge identifier
   * @param label       edge label
   * @param sourceId    source vertex id
   * @param targetId    target vertex id
   * @param properties  edge properties
   * @param graphIds    graphs that edge is contained in
   */
  Edge(
    final GradoopId id,
    final String label,
    final GradoopId sourceId,
    final GradoopId targetId,
    final PropertyList properties,
    GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getSourceId() {
    return sourceId;
  }

  /**
   * {@inheritDoc}
   * @param sourceId
   */
  @Override
  public void setSourceId(GradoopId sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getTargetId() {
    return targetId;
  }

  /**
   * {@inheritDoc}
   * @param targetId
   */
  @Override
  public void setTargetId(GradoopId targetId) {
    this.targetId = targetId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return String.format("(%s)-[%s]->(%s)",
      sourceId, super.toString(), targetId);
  }
}
