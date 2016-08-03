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
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;

import java.io.Serializable;

/**
 * Factory for creating graph head POJOs.
 */
public class GraphHeadFactory implements EPGMGraphHeadFactory<GraphHead>,
  Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead createGraphHead() {
    return initGraphHead(GradoopId.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead initGraphHead(final GradoopId id) {
    return initGraphHead(id, GConstants.DEFAULT_GRAPH_LABEL, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead createGraphHead(String label) {
    return initGraphHead(GradoopId.get(), label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead initGraphHead(final GradoopId id, final String label) {
    return initGraphHead(id, label, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead createGraphHead(String label, PropertyList properties) {
    return initGraphHead(GradoopId.get(), label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead initGraphHead(final GradoopId id, final String label,
    PropertyList properties) {
    Preconditions.checkNotNull(id, "Identifier was null");
    Preconditions.checkNotNull(label, "Label was null");
    return new GraphHead(id, label, properties);
  }

  @Override
  public Class<GraphHead> getType() {
    return GraphHead.class;
  }
}
