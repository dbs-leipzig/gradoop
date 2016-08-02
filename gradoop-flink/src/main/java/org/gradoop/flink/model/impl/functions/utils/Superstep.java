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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Returns the current superstep using {@link IterationRuntimeContext}.
 *
 * Note that this function can only be applied in an iterative context (i.e.
 * bulk or delta iteration).
 *
 * @param <T> input type
 */
public class Superstep<T> extends RichMapFunction<T, Integer> {

  /**
   * super step
   */
  private Integer superstep;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    superstep = getIterationRuntimeContext().getSuperstepNumber();
  }

  @Override
  public Integer map(T value) throws Exception {
    return superstep;
  }
}
