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

package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Checks if a given object of type {@link IN} is instance of a specific class
 * of type {@link T}.
 *
 * @param <IN>  input type
 * @param <T>   class type to check
 */
public class IsInstance<IN, T> implements FilterFunction<IN> {
  /**
   * Class for isInstance check
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for isInstance check
   */
  public IsInstance(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public boolean filter(IN value) throws Exception {
    return clazz.isInstance(value);
  }
}
