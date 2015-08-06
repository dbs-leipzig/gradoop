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

import org.gradoop.model.EdgeData;

import java.util.Map;
import java.util.Set;

/**
 * Transient representation of edge data.
 */
public class DefaultEdgeData extends EPGMElement implements EdgeData {

  private Set<Long> graphs;

  private Long sourceVertexId;

  private Long targetVertexId;

  public DefaultEdgeData() {
  }

  /**
   * Creates an edge instance based on the given parameters.
   *
   * @param id             the unique id of the edge
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphs         graphs that edge is contained in
   */
  DefaultEdgeData(final Long id, final String label, final Long sourceVertexId,
    final Long targetVertexId, final Map<String, Object> properties,
    Set<Long> graphs) {
    super(id, label, properties);
    this.graphs = graphs;
    this.sourceVertexId = sourceVertexId;
    this.targetVertexId = targetVertexId;
  }

  @Override
  public Long getSourceVertexId() {
    return sourceVertexId;
  }

  @Override
  public void setSourceVertexId(Long sourceVertexId) {
    this.sourceVertexId = sourceVertexId;
  }

  @Override
  public Long getTargetVertexId() {
    return targetVertexId;
  }

  @Override
  public void setTargetVertexId(Long targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  @Override
  public Set<Long> getGraphs() {
    return graphs;
  }

  @Override
  public void addGraph(Long graph) {
    graphs.add(graph);
  }

  @Override
  public void setGraphs(Set<Long> graphs) {
    this.graphs = graphs;
  }

  @Override
  public void resetGraphs() {
    graphs.clear();
  }

  @Override
  public int getGraphCount() {
    return graphs.size();
  }

  @Override
  public String toString() {
    return "DefaultEdgeData{" +
      super.toString() +
      ", graphs=" + graphs +
      ", sourceVertexId=" + sourceVertexId +
      ", targetVertexId=" + targetVertexId +
      '}';
  }
}
