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
import org.gradoop.model.EdgeDataFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating edges.
 */
public class DefaultEdgeDataFactory implements EdgeDataFactory<DefaultEdgeData>,
  Serializable {

  @Override
  public DefaultEdgeData createEdgeData(final Long id,
    final Long sourceVertexId, final Long targetVertexId) {
    return createEdgeData(id, GConstants.DEFAULT_EDGE_LABEL, sourceVertexId,
      targetVertexId);
  }

  @Override
  public DefaultEdgeData createEdgeData(final Long id, final String label,
    final Long sourceVertexId, final Long targetVertexId) {
    return createEdgeData(id, label, sourceVertexId, targetVertexId, null,
      null);
  }

  @Override
  public DefaultEdgeData createEdgeData(Long id, String label,
    Long sourceVertexId, Long targetVertexId, Map<String, Object> properties) {
    return createEdgeData(id, label, sourceVertexId, targetVertexId, properties,
      null);
  }

  @Override
  public DefaultEdgeData createEdgeData(final Long id, final String label,
    final Long sourceVertexId, final Long targetVertexId, Set<Long> graphs) {
    return createEdgeData(id, label, sourceVertexId, targetVertexId, null,
      graphs);
  }

  @Override
  public DefaultEdgeData createEdgeData(final Long id, final String label,
    final Long sourceVertexId, final Long targetVertexId,
    final Map<String, Object> properties, Set<Long> graphs) {
    checkID(id);
    checkLabel(label);
    checkID(sourceVertexId);
    checkID(targetVertexId);
    return new DefaultEdgeData(id, label, sourceVertexId, targetVertexId,
      properties, graphs);
  }

  /**
   * Checks if {@code otherID} is valid.
   *
   * @param otherID id of entity that edge points to
   */
  private static void checkID(Long otherID) {
    if (otherID == null) {
      throw new IllegalArgumentException(
        "edge-, source-, target-id must not be null");
    }
  }

  /**
   * Checks if {@code label} is valid (not null or empty).
   *
   * @param label edge label
   */
  private static void checkLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
  }

  @Override
  public Class<DefaultEdgeData> getType() {
    return DefaultEdgeData.class;
  }
}
