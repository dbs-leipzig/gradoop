package org.gradoop.flink.algorithms.fsm2.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

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

      Collection<String> siblings = sortedLabels.get(frequency);

      if (siblings == null) {
        siblings = new TreeSet<>();
        sortedLabels.put(frequency, siblings);
      }

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
