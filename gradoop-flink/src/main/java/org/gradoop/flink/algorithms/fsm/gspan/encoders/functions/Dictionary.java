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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.comparators.LabelFrequencyComparator;


import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collections;
import java.util.List;

/**
 * creates a label dictionary based on support (max support = label 1)
 *
 * (label, support),.. => (label, 1), (label, 2)
 */
public class Dictionary
  implements GroupReduceFunction<WithCount<String>, List<String>> {

  @Override
  public void reduce(Iterable<WithCount<String>> iterable,
    Collector<List<String>> collector) throws Exception {

    List<WithCount<String>> list = Lists.newArrayList();

    for (WithCount<String> labelCount : iterable) {
      list.add(labelCount);
    }

    Collections.sort(list, new LabelFrequencyComparator());

    List<String> intStringDictionary = Lists
      .newArrayListWithCapacity(list.size());

    for (WithCount<String> entry : list) {
      intStringDictionary.add(entry.getObject());
    }

    collector.collect(intStringDictionary);
  }
}
