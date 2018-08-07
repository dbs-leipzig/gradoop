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
