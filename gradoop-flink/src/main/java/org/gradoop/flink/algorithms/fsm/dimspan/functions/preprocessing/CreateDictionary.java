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

package org.gradoop.flink.algorithms.fsm.dimspan.functions.preprocessing;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.LabelComparator;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;

/**
 * (label, frequency),.. => [label,..]
 */
public class CreateDictionary implements GroupReduceFunction<WithCount<String>, String[]> {

  /**
   * comparator used to determine frequency-dependent translation
   */
  private final LabelComparator comparator;

  /**
   * Constructor.
   *
   * @param comparator label comparator
   */
  public CreateDictionary(LabelComparator comparator) {
    this.comparator = comparator;
  }

  @Override
  public void reduce(
    Iterable<WithCount<String>> iterable, Collector<String[]> collector) throws Exception {

    // Sort distinct labels
    List<WithCount<String>> stringsWithCount = Lists.newArrayList(iterable);
    stringsWithCount.sort(comparator);

    // Create dictionary with default label
    List<String> dictionary = Lists.newArrayList();

    for (WithCount<String> stringWithCount : stringsWithCount) {
      dictionary.add(stringWithCount.getObject());
    }

    collector.collect(dictionary.toArray(new String[dictionary.size()]));
  }
}
