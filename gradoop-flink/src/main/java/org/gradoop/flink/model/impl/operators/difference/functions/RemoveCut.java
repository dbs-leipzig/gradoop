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

package org.gradoop.flink.model.impl.operators.difference.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * If a group only contains one element, return it. Else return nothing.
 *
 * @param <O> any object type
 */
public class RemoveCut<O>
  implements GroupReduceFunction<Tuple2<O, Long>, O> {

  @Override
  public void reduce(Iterable<Tuple2<O, Long>> iterable,
    Collector<O> collector) throws Exception {
    boolean inFirst = false;
    boolean inSecond = false;

    O o = null;

    for (Tuple2<O, Long> tuple : iterable) {
      o = tuple.f0;
      if (tuple.f1 == 1L) {
        inFirst = true;
      } else {
        inSecond = true;
      }
    }
    if (inFirst && !inSecond) {
      collector.collect(o);
    }
  }
}
