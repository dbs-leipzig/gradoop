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

    boolean first = true;
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
