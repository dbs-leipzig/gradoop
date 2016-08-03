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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * creates a string representation of an adjacency matrix
 */
public class AdjacencyMatrix implements
  GroupReduceFunction<VertexString, GraphHeadString> {

  @Override
  public void reduce(Iterable<VertexString> vertexLabels,
    Collector<GraphHeadString> collector) throws Exception {

    Boolean first = true;
    GradoopId graphId = null;

    List<String> matrixRows = new ArrayList<>();

    for (VertexString vertexString : vertexLabels) {
      if (first) {
        graphId = vertexString.getGraphId();
        first = false;
      }

      matrixRows.add("\n " + vertexString.getLabel());
    }

    Collections.sort(matrixRows);

    collector.collect(
      new GraphHeadString(graphId, StringUtils.join(matrixRows, "")));
  }
}
