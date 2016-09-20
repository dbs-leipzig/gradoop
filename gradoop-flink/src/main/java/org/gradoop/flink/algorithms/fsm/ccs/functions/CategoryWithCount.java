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

package org.gradoop.flink.algorithms.fsm.ccs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.ccs.pojos.CCSGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * graph => (category, 1)
 */
public class CategoryWithCount
  implements MapFunction<CCSGraph, WithCount<String>> {

  /**
   * reuse tuple to avoid instantiations
   */
  private WithCount<String> reuseTuple = new WithCount<>(null);

  @Override
  public WithCount<String> map(CCSGraph value) throws Exception {
    reuseTuple.setObject(value.getCategory());
    return reuseTuple;
  }
}
