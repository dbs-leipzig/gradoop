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

package org.gradoop.model.api.functions;

import org.gradoop.model.api.EPGMAttributed;
import org.gradoop.model.api.EPGMLabeled;

/**
 * A modification function is applied on an EPGM element (i.e. graph head,
 * vertex and edge) to modify its data, but not its identity.
 *
 * @param <EL> EPGM attributed / labeled element
 */
public interface ModificationFunction<EL extends EPGMAttributed & EPGMLabeled>
  extends BinaryFunction<EL, EL, EL> {

  /**
   * The method takes the current version of the element and a copy of that
   * element as input. The copy is initialized with the current structural
   * information (i.e. identifiers, graph membership, source / target
   * identifiers). The implementation is able to modify the element by either
   * updating the current version and return it or by adding necessary
   * information to the new entity and return it.
   *
   * @param current   current element
   * @param modified  structural identical, but plain element
   * @return modified version of current element
   */
  @Override
  EL execute(EL current, EL modified);
}
