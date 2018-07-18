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
