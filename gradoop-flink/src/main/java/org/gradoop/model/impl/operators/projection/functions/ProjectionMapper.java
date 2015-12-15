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
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.functions.ProjectionFunction;

/**
 * Applies a given projection function on the input EPGM element.
 *
 * @param <EL> EPGM element
 */
public class ProjectionMapper<EL extends EPGMElement>
  implements MapFunction<EL, EL> {
  /**
   * Edge to edge function
   */
  private ProjectionFunction<EL> edgeFunc;

  /**
   * Create a new ProjectEdgesMapper
   *
   * @param func the edge projection function
   */
  public ProjectionMapper(ProjectionFunction<EL> func) {
    this.edgeFunc = func;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(EL edge) throws Exception {
    return edgeFunc.execute(edge);
  }
}
