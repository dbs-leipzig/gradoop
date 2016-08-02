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

package org.gradoop.flink.model.impl.operators.intersection.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;

import java.util.Iterator;

/**
 * If the number of grouped elements equals a given expected size, return the
 * last element.
 *
 * @param <O> any object type
 * @see Intersection
 */
public class GroupCountEquals<O> implements GroupReduceFunction<O, O> {

  /**
   * User defined expectedGroupSize.
   */
  private final long expectedGroupSize;

  /**
   * Constructor
   *
   * @param expectedGroupSize expected group size
   */
  public GroupCountEquals(long expectedGroupSize) {
    this.expectedGroupSize = expectedGroupSize;
  }

  /**
   * If the number of elements in the group is equal to the user expected
   * group size, the last element is returned.
   *
   * @param iterable  graph data
   * @param collector output collector (contains 0 or 1 graph)
   * @throws Exception
   */
  @Override
  public void reduce(Iterable<O> iterable,
    Collector<O> collector) throws Exception {
    Iterator<O> iterator = iterable.iterator();
    long count = 0L;
    O object = null;
    while (iterator.hasNext()) {
      object = iterator.next();
      count++;
    }
    if (count == expectedGroupSize) {
      collector.collect(object);
    }
  }
}
