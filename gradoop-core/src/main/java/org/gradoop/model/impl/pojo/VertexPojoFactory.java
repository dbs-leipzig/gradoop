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

import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMVertexFactory;

import java.util.Map;
import java.util.Set;

/**
 * Factory for creating vertices.
 */
public class VertexPojoFactory extends ElementPojoFactory
  implements EPGMVertexFactory<VertexPojo> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(final GradoopId vertexID) {
    return createVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(final GradoopId vertexID, final String label) {
    return createVertex(vertexID, label, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(final GradoopId vertexID, final String label,
    Map<String, Object> properties) {
    return createVertex(vertexID, label, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(final GradoopId vertexID, final String label,
    final GradoopIds graphs) {
    return createVertex(vertexID, label, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VertexPojo createVertex(final GradoopId id, final String label,
    final Map<String, Object> properties, final GradoopIds graphs) {
    checkId(id);
    checkLabel(label);
    return new VertexPojo(id, label, properties, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<VertexPojo> getType() {
    return VertexPojo.class;
  }
}
