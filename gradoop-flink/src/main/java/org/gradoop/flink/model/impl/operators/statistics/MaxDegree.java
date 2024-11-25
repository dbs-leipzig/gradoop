/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.AddSumDegreesToGraphHeadCrossFunction;

/**
 * Max degree operator calculates the maximum degree of all vertices of a graph and writes it to the graph
 * head as a new property named {@link MaxDegree#PROPERTY_MAX_DEGREE}.
 */
public class MaxDegree implements UnaryBaseGraphToBaseGraphOperator<LogicalGraph> {

  /**
   * The name of the property that holds the max degree after the calculation.
   */
  public static final String PROPERTY_MAX_DEGREE = "maxDegree";

  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    DataSet<EPGMGraphHead> newGraphHead = new VertexDegrees().execute(graph)
      .max(1)
      .crossWithTiny(graph.getGraphHead().first(1))
      .with(new AddSumDegreesToGraphHeadCrossFunction(PROPERTY_MAX_DEGREE));

    return graph.getFactory().fromDataSets(newGraphHead, graph.getVertices(), graph.getEdges());
  }
}
