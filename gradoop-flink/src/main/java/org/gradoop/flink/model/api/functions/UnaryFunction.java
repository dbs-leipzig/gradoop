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

import java.io.Serializable;

/**
 * Defines a function with single input and output.
 *
 * @param <I> input type
 * @param <O> output type
 */
public interface UnaryFunction<I, O> extends Serializable {
  /**
   * Creates output from given input.
   *
   * @param entity some entity
   * @return some object
   * @throws Exception
   */
  O execute(I entity) throws Exception;
}
