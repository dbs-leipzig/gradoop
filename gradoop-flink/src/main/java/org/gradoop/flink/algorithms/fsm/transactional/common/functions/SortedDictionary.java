
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

/**
 * (label, frequency),.. => label->translation
 * higher frequency leads to lower label
 */
public class SortedDictionary
  implements GroupReduceFunction<WithCount<String>, Map<String, Integer>> {

  @Override
  public void reduce(Iterable<WithCount<String>> iterable,
    Collector<Map<String, Integer>> collector) throws Exception {

    Map<String, Integer> dictionary = Maps.newHashMap();
    Map<Long, Collection<String>> sortedLabels = Maps.newTreeMap();

    for (WithCount<String> labelFrequency : iterable) {

      String label = labelFrequency.getObject();
      Long frequency = labelFrequency.getCount();

      Collection<String> siblings = sortedLabels.computeIfAbsent(frequency, k -> new TreeSet<>());

      siblings.add(label);
    }

    int translation = 0;
    for (Collection<String> siblings : sortedLabels.values()) {
      for (String label : siblings) {
        dictionary.put(label, translation);
        translation++;
      }
    }

    collector.collect(dictionary);
  }
}
