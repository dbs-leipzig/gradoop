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

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Casts an object of type {@link IN} to type {@link T}.
 *
 * @param <IN>  input type
 * @param <T>   type to cast to
 */
public class Cast<IN, T> implements MapFunction<IN, T> {

  /**
   * Class for type cast
   */
  private final Class<T> clazz;

  /**
   * Constructor
   *
   * @param clazz class for type cast
   */
  public Cast(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T map(IN value) throws Exception {
    return clazz.cast(value);
  }
}
