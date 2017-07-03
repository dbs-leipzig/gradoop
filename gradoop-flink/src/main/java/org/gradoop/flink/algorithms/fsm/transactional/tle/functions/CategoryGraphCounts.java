
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
