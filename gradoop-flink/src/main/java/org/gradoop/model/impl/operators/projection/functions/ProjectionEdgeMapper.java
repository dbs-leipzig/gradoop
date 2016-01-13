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

package org.gradoop.model.impl.operators.projection.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.functions.ProjectionFunction;

/**
 * Applies a given projection function on the input EPGM edge.
 *
 * @param <E> EPGM edge type
 */
public class ProjectionEdgeMapper<E extends EPGMEdge>
  implements MapFunction<E, E> {
  /**
   * Edge projection function
   */
  private ProjectionFunction<E> func;

  /**
   * Factory for creation of new edges
   */
  private EPGMEdgeFactory<E> factory;

  /**
   * Create a new ProjectionEdgeMapper
   *
   * @param factory factory for creation of new edges
   * @param func the projection function
   */
  public ProjectionEdgeMapper(EPGMEdgeFactory<E> factory,
    ProjectionFunction<E> func) {
    this.factory = factory;
    this.func = func;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public E map(E edge) throws Exception {
    return func.execute(
      edge, factory.createEdge(edge.getSourceId(), edge.getTargetId()));
  }
}
