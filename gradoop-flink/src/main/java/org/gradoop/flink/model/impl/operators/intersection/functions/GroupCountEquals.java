/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
