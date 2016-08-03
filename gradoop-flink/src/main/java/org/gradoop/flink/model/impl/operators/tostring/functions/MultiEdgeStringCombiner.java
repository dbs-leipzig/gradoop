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
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * combines string representations of multiple (parallel) edges
 */
public class MultiEdgeStringCombiner implements
  GroupReduceFunction<EdgeString, EdgeString> {

  @Override
  public void reduce(Iterable<EdgeString> iterable,
    Collector<EdgeString> collector) throws Exception {

    List<String> labels = new ArrayList<>();
    Boolean first = true;
    EdgeString combinedLabel = null;

    for (EdgeString edgeString : iterable) {
      if (first) {
        combinedLabel = edgeString;
        first = false;
      }

      labels.add(edgeString.getEdgeLabel());
    }

    Collections.sort(labels);

    if (combinedLabel != null) {
      combinedLabel.setEdgeLabel(StringUtils.join(labels, "&"));
    }

    collector.collect(combinedLabel);
  }
}
