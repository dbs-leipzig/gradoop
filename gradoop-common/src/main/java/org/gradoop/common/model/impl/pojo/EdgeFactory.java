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

import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory for creating edge POJOs.
 */
public class EdgeFactory implements EPGMEdgeFactory<Edge>, Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final GradoopId sourceVertexId,
    final GradoopId targetVertexId) {
    return initEdge(id, GConstants.DEFAULT_EDGE_LABEL, sourceVertexId,
      targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, PropertyList properties) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(
    GradoopId id,
    String label,
    GradoopId sourceVertexId,
    GradoopId targetVertexId,
    PropertyList properties) {

    return
      initEdge(id, label, sourceVertexId, targetVertexId, properties, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId,
    GradoopIdSet graphs) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge createEdge(String label, GradoopId sourceVertexId,
    GradoopId targetVertexId, PropertyList properties,
    GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
      label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge initEdge(final GradoopId id, final String label,
    final GradoopId sourceVertexId, final GradoopId targetVertexId,
    final PropertyList properties, GradoopIdSet graphIds) {
    checkNotNull(id, "Identifier was null");
    checkNotNull(label, "Label was null");
    checkNotNull(sourceVertexId, "Source vertex id was null");
    checkNotNull(targetVertexId, "Target vertex id was null");
    return new Edge(id, label, sourceVertexId, targetVertexId,
      properties, graphIds);
  }

  @Override
  public Class<Edge> getType() {
    return Edge.class;
  }
}
