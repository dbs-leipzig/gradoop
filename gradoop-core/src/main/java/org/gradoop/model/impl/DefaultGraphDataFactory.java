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
import org.gradoop.model.GraphDataFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Factory for creating graph data.
 */
public class DefaultGraphDataFactory extends
  DefaultEPGMElementFactory implements GraphDataFactory<DefaultGraphData>,
  Serializable {

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultGraphData createGraphData(final Long id) {
    return createGraphData(id, GConstants.DEFAULT_GRAPH_LABEL, null, null,
      null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultGraphData createGraphData(final Long id, final String label) {
    return createGraphData(id, label, null, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultGraphData createGraphData(final Long id, final String label,
    Map<String, Object> properties) {
    return createGraphData(id, label, properties, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultGraphData createGraphData(Long id, String label,
    Set<Long> vertices, Set<Long> edges) {
    return createGraphData(id, label, null, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DefaultGraphData createGraphData(final Long id, final String label,
    final Map<String, Object> properties, final Set<Long> vertices,
    final Set<Long> edges) {
    checkId(id);
    checkLabel(label);
    return new DefaultGraphData(id, label, properties, vertices, edges);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<DefaultGraphData> getType() {
    return DefaultGraphData.class;
  }
}
