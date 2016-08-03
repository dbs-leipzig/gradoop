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

package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * A transformation function is applied on an EPGM element (i.e. graph head,
 * vertex and edge) to transform its data, but not its identity.
 *
 * @param <EL> EPGM attributed / labeled element
 */
public interface TransformationFunction<EL extends EPGMAttributed & EPGMLabeled>
  extends BinaryFunction<EL, EL, EL> {

  /**
   * The method takes the current version of the element and a copy of that
   * element as input. The copy is initialized with the current structural
   * information (i.e. identifiers, graph membership, source / target
   * identifiers). The implementation is able to transform the element by either
   * updating the current version and return it or by adding necessary
   * information to the new entity and return it.
   *
   * @param current       current element
   * @param transformed   structural identical, but plain element
   * @return transformed element
   */
  @Override
  EL execute(EL current, EL transformed);
}
