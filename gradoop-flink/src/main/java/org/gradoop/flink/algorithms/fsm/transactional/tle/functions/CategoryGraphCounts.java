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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Map;

/**
 * Creates a map of graph counts per category.
 */
public class CategoryGraphCounts implements
  GroupReduceFunction<WithCount<String>, Map<String, Long>> {

  @Override
  public void reduce(Iterable<WithCount<String>> categoriesWithCount,
    Collector<Map<String, Long>> out) throws Exception {

    Map<String, Long> categoryCounts = Maps.newHashMap();

    for (WithCount<String> categoryWithCount : categoriesWithCount) {

      String category = categoryWithCount.getObject();
      long count = categoryWithCount.getCount();

      Long sum = categoryCounts.get(category);

      if (sum == null) {
        sum = count;
      } else {
        sum += count;
      }

      categoryCounts.put(category, sum);
    }

    out.collect(categoryCounts);
  }
}
