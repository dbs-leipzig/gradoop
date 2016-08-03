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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Iterator;

/**
 * (t, count1),..,(t, countN) => (t, SUM(count1,..,countN))
 *
 * @param <T> data type
 */
public class SumCount<T>
  implements GroupCombineFunction<WithCount<T>, WithCount<T>> {

  @Override
  public void combine(Iterable<WithCount<T>> iterable,
    Collector<WithCount<T>> collector) throws Exception {

    Iterator<WithCount<T>> iterator = iterable.iterator();
    WithCount<T> withCount = iterator.next();

    while (iterator.hasNext()) {
      withCount.setCount(withCount.getCount() + iterator.next().getCount());
    }

    collector.collect(withCount);
  }
}
