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
