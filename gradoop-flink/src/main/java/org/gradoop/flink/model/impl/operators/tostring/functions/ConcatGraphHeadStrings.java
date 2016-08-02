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

package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * concatenates the sorted string representations of graph heads to represent a
 * collection
 */
public class ConcatGraphHeadStrings
  implements GroupReduceFunction<GraphHeadString, String> {

  @Override
  public void reduce(Iterable<GraphHeadString> graphHeadLabels,
    Collector<String> collector) throws Exception {

    List<String> graphLabels = new ArrayList<>();

    for (GraphHeadString graphHeadString : graphHeadLabels) {
      String graphLabel = graphHeadString.getLabel();
      if (! graphLabel.equals("")) {
        graphLabels.add(graphLabel);
      }
    }

    Collections.sort(graphLabels);

    collector.collect(StringUtils.join(graphLabels, "\n"));
  }
}
