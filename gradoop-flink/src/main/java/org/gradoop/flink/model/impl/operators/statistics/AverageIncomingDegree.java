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
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingEvaluationConstants;
import org.gradoop.flink.model.impl.operators.statistics.functions.AddSumDegreesToGraphHeadCrossFunction;
import org.gradoop.flink.model.impl.operators.statistics.functions.CalculateAverageDegree;

/**
 * Calculates the average incoming degree of a graph and writes it to the graph head.
 * Uses: {@code ceiling( sum(vertex incoming degrees) / |vertices| )}
 */
public class AverageIncomingDegree implements UnaryGraphToGraphOperator {

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    graph = graph.aggregate(new VertexCount());
    DataSet<EPGMGraphHead> newGraphHead = new IncomingVertexDegrees().execute(graph)
      .sum(1)
      .crossWithTiny(graph.getGraphHead().first(1))
      .with(new AddSumDegreesToGraphHeadCrossFunction(
        SamplingEvaluationConstants.PROPERTY_KEY_SUM_DEGREES))
      .map(new CalculateAverageDegree(
        SamplingEvaluationConstants.PROPERTY_KEY_AVERAGE_INCOMING_DEGREE));

    return graph.getFactory()
      .fromDataSets(newGraphHead, graph.getVertices(), graph.getEdges());
  }
}
