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

package org.gradoop.flink.model.impl.operators.statistics.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Returns the first value of the join pair if it exists, otherwise a new {@link WithCount} with
 * count = 0 is created.
 */
public class SetOrCreateWithCount
  implements JoinFunction<WithCount<GradoopId>, GradoopId, WithCount<GradoopId>> {
  /**
   * Reduce object instantiations
   */
  private final WithCount<GradoopId> reuseTuple;
  /**
   * Constructor
   */
  public SetOrCreateWithCount() {
    reuseTuple = new WithCount<>();
    reuseTuple.setCount(0L);
  }

  @Override
  public WithCount<GradoopId> join(WithCount<GradoopId> first, GradoopId second) throws Exception {
    if (first == null) {
      reuseTuple.setObject(second);
      return reuseTuple;
    } else {
      return first;
    }
  }
}
