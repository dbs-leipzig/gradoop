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
