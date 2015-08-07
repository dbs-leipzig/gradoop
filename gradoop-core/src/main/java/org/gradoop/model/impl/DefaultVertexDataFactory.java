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
import org.gradoop.model.VertexDataFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating vertices.
 */
public class DefaultVertexDataFactory implements
  VertexDataFactory<DefaultVertexData>, Serializable {

  @Override
  public DefaultVertexData createVertexData(final Long vertexID) {
    return createVertexData(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null,
      null);
  }

  @Override
  public DefaultVertexData createVertexData(final Long vertexID,
    final String label) {
    return createVertexData(vertexID, label, null, null);
  }

  @Override
  public DefaultVertexData createVertexData(final Long vertexID,
    final String label, Map<String, Object> properties) {
    return createVertexData(vertexID, label, properties, null);
  }

  @Override
  public DefaultVertexData createVertexData(final Long vertexID,
    final String label, final Set<Long> graphs) {
    return createVertexData(vertexID, label, null, graphs);
  }

  @Override
  public DefaultVertexData createVertexData(final Long id, final String label,
    final Map<String, Object> properties, final Set<Long> graphs) {
    checkVertexID(id);
    checkLabel(label);
    return new DefaultVertexData(id, label, properties, graphs);
  }

  /**
   * Checks if the given vertexID is valid.
   *
   * @param vertexID vertex identifier
   */
  private void checkVertexID(final Long vertexID) {
    if (vertexID == null) {
      throw new IllegalArgumentException("vertexID must not be null");
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
  public Class<DefaultVertexData> getType() {
    return DefaultVertexData.class;
  }
}
