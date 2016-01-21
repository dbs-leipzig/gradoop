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

package org.gradoop.model.impl.operators.modification.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.functions.ModificationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Initializes the current version and executes the modification function.
 *
 * @param <EL> EPGM element type
 */
public abstract class ModifyBase<EL extends EPGMElement>
  implements MapFunction<EL, EL> {

  /**
   * Element modification function
   */
  private final ModificationFunction<EL> modificationFunction;

  /**
   * Constructor
   *
   * @param modificationFunction element modification function
   */
  protected ModifyBase(ModificationFunction<EL> modificationFunction) {
    this.modificationFunction = checkNotNull(modificationFunction);
  }

  /**
   * Applies the modification function on the current element and its copy.
   *
   * @param element current element
   * @return modified element
   * @throws Exception
   */
  @Override
  public EL map(EL element) throws Exception {
    return modificationFunction.execute(element, initFrom(element));
  }

  /**
   * Initializes the new element from the current element.
   *
   * @param element current element
   * @return current element with identical structure but plain data
   */
  protected abstract EL initFrom(EL element);
}
