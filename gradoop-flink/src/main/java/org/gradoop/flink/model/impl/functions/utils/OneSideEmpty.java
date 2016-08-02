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

import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Evaluates to true, if one join partner is NULL.
 * @param <L> left type
 * @param <R> right type
 */
public class OneSideEmpty<L, R> implements JoinFunction<L, R, Boolean> {

  @Override
  public Boolean join(L left, R right) throws Exception {
    return left == null || right == null;
  }
}
