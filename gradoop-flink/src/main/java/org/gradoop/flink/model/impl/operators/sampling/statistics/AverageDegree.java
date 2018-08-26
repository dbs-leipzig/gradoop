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
package org.gradoop.flink.model.impl.operators.sampling.statistics;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.CalculateAverageDegree;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Calculates the average degree of a graph and writes it to the graph head.
 * Uses: ceiling( sum(vertex degrees) / |vertices| )
 */
public class AverageDegree implements UnaryGraphToGraphOperator {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    graph = graph.aggregate(new VertexCount());
    DataSet<GraphHead> newGraphHead = new VertexDegrees().execute(graph)
      .sum(1)
      .cross(graph.getGraphHead().first(1))
      .with(new CrossFunction<WithCount<GradoopId>, GraphHead, GraphHead>() {
        @Override
        public GraphHead cross(WithCount<GradoopId> sumDeg, GraphHead graphHead) throws Exception {
          graphHead.setProperty(
            SamplingEvaluationConstants.PROPERTY_KEY_SUM_DEGREES, sumDeg.getCount());
          return graphHead;
        }
      })
      .map(new CalculateAverageDegree(SamplingEvaluationConstants.PROPERTY_KEY_AVERAGE_DEGREE));

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(newGraphHead, graph.getVertices(), graph.getEdges());
  }

  @Override
  public String getName() {
    return AverageDegree.class.getName();
  }
}
