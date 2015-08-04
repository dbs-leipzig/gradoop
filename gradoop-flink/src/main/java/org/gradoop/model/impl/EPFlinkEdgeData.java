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

import org.gradoop.model.EPEdgeData;

import java.util.Map;
import java.util.Set;

/**
 * POJO for edge data.
 */
public class EPFlinkEdgeData extends EPFlinkGraphElementEntity implements
  EPEdgeData {
  private Long sourceVertex;
  private Long targetVertex;

  public EPFlinkEdgeData() {
  }

  public EPFlinkEdgeData(EPFlinkEdgeData otherEdgeData) {
    super(otherEdgeData);
    this.sourceVertex = otherEdgeData.sourceVertex;
    this.targetVertex = otherEdgeData.targetVertex;
  }

  public EPFlinkEdgeData(Long id, String label, Long sourceVertex,
    Long targetVertex) {
    this(id, label, sourceVertex, targetVertex, null, null);
  }

  public EPFlinkEdgeData(Long id, String label, Long sourceVertex,
    Long targetVertex, Map<String, Object> properties) {
    super(id, label, properties, null);
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  public EPFlinkEdgeData(Long id, String label, Long sourceVertex,
    Long targetVertex, Set<Long> graphs) {
    super(id, label, null, graphs);
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  public EPFlinkEdgeData(Long id, String label, Long sourceVertex,
    Long targetVertex, Map<String, Object> properties, Set<Long> graphs) {
    super(id, label, properties, graphs);
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  @Override
  public Long getSourceVertex() {
    return sourceVertex;
  }

  @Override
  public void setSourceVertex(Long sourceVertexId) {
    this.sourceVertex = sourceVertexId;
  }

  @Override
  public Long getTargetVertex() {
    return targetVertex;
  }

  @Override
  public void setTargetVertex(Long targetVertexId) {
    this.targetVertex = targetVertexId;
  }

  @Override
  public String toString() {
    return "EPFlinkEdgeData{" +
      "super=" + super.toString() +
      ", sourceVertex=" + sourceVertex +
      ", targetVertex=" + targetVertex +
      '}';
  }
}
