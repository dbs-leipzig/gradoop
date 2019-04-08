/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryGraphToValueOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.CalculateDegreeCentrality;
import org.gradoop.flink.model.impl.operators.statistics.functions.DegreeDistanceFunction;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes the degree centrality of a graph
 */
public class DegreeCentrality extends DegreeCentralityBase
  implements UnaryGraphToValueOperator<DataSet<Double>> {

  /**
   * Calculates the degree centrality of the graph
   * using the function: (Sum(d(max) - d(i)) / (v_count -2) * (v_count-1)
   *
   * @param graph input graph
   * @return DataSet with one degree centrality value
   */
  @Override
  public DataSet<Double> execute(LogicalGraph graph) {

    DataSet<WithCount<GradoopId>> degrees = new VertexDegrees().execute(graph);
    DataSet<Long> vertexCount = new VertexCount().execute(graph);

    // for broadcasting
    DataSet<WithCount<GradoopId>> maxDegree = degrees.max(1);

    return degrees
      .map(new DegreeDistanceFunction(BROADCAST_NAME))
      .withBroadcastSet(maxDegree, BROADCAST_NAME)
      .sum(0)
      .crossWithTiny(vertexCount).with(new CalculateDegreeCentrality());
  }

  @Override
  public String getName() {
    return DegreeCentrality.class.getName();
  }
}

