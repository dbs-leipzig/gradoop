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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.sampling.statistics.functions.CalculateDensity;

/**
 * Computes the density of a graph and writes it to the graph head.
 * Uses: (|E|) / (|V| * (|V| - 1))
 */
public class GraphDensity implements UnaryGraphToGraphOperator {

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    DataSet<GraphHead> newGraphHead = graph
      .aggregate(new VertexCount(), new EdgeCount())
      .getGraphHead()
      .map(new CalculateDensity(SamplingEvaluationConstants.PROPERTY_KEY_DENSITY));

    return graph.getConfig().getLogicalGraphFactory()
      .fromDataSets(newGraphHead, graph.getVertices(), graph.getEdges());
  }

  @Override
  public String getName() {
    return GraphDensity.class.getName();
  }
}
