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

package org.gradoop.model.impl.operators.equality.functions;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMLabeled;

import java.util.Collections;
import java.util.List;

/**
 * "Z", "A", "M" => "AMZ"
 *
 * @param <L> labeled type
 */
public class SortAndConcatLabels<L extends EPGMLabeled>
  implements GroupReduceFunction<L, L> {
  @Override
  public void reduce(Iterable<L> iterable,
    Collector<L> collector) throws Exception {

    List<L> fatLabels = Lists.newArrayList(iterable);

    if (!fatLabels.isEmpty()) {
      List<String> labels = Lists.newArrayList();

      for (L fatLabel : fatLabels) {
        labels.add(fatLabel.getLabel());
      }

      Collections.sort(labels);

      L concatenatedFatLabel = fatLabels.get(0);
      concatenatedFatLabel.setLabel(StringUtils.join(labels, ","));

      collector.collect(concatenatedFatLabel);
    }
  }
}
