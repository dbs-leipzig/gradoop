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

import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.api.EPGMProperties;
import org.gradoop.util.GConstants;

/**
 * Factory for creating graph data.
 */
public class GraphHeadPojoFactory extends ElementPojoFactory implements EPGMGraphHeadFactory<GraphHeadPojo> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo createGraphHead() {
    return initGraphHead(GradoopId.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo initGraphHead(final GradoopId id) {
    return initGraphHead(id, GConstants.DEFAULT_GRAPH_LABEL, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo createGraphHead(String label) {
    return initGraphHead(GradoopId.get(), label);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo initGraphHead(final GradoopId id, final String label) {
    return initGraphHead(id, label, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo createGraphHead(String label,
    EPGMProperties properties) {
    return initGraphHead(GradoopId.get(), label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHeadPojo initGraphHead(final GradoopId id, final String label,
    EPGMProperties properties) {
    checkId(id);
    checkLabel(label);
    return new GraphHeadPojo(id, label, properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<GraphHeadPojo> getType() {
    return GraphHeadPojo.class;
  }
}
