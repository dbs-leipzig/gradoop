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

import com.google.common.base.Preconditions;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

import java.io.Serializable;

/**
 * Factory for creating vertex POJOs.
 */
public class VertexFactory implements EPGMVertexFactory<Vertex>, Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex() {
    return initVertex(GradoopId.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID) {
    return initVertex(vertexID, GConstants.DEFAULT_VERTEX_LABEL, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label) {
    return initVertex(vertexID, label, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, PropertyList properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label,
    PropertyList properties) {
    return initVertex(vertexID, label, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId vertexID, final String label,
    final GradoopIdSet graphs) {
    return initVertex(vertexID, label, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex createVertex(String label, PropertyList properties,
    GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex initVertex(final GradoopId id, final String label,
    final PropertyList properties, final GradoopIdSet graphs) {
    Preconditions.checkNotNull(id, "Identifier was null");
    Preconditions.checkNotNull(label, "Label was null");
    return new Vertex(id, label, properties, graphs);
  }

  @Override
  public Class<Vertex> getType() {
    return Vertex.class;
  }
}
