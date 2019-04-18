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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.ClusteringCoefficientBase;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.GellyLocalClusteringCoefficientDirected;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.model.impl.operators.statistics.functions.AddAverageCCValueToGraphHeadMap;

/**
 * Calculates the average local clustering coefficient of a graph and writes it to the graph head.
 * Uses the Gradoop EPGM model wrapper for Flink Gellys implementation of the local clustering
 * coefficient algorithm for directed graphs {@link GellyLocalClusteringCoefficientDirected}
 */
public class AverageClusteringCoefficient implements UnaryGraphToGraphOperator {

  /**
   * Property key to access the average clustering coefficient value stored the graph head
   */
  public static final String PROPERTY_KEY_AVERAGE = "clustering_coefficient_average";

  @Override
  public LogicalGraph execute(LogicalGraph graph) {

    VertexCount vertexCountAggregate = new VertexCount();
    SumVertexProperty localCCAggregate = new SumVertexProperty(
      ClusteringCoefficientBase.PROPERTY_KEY_LOCAL);

    graph = new GellyLocalClusteringCoefficientDirected().execute(graph)
      .aggregate(vertexCountAggregate, localCCAggregate);

    DataSet<GraphHead> graphHead = graph.getGraphHead().map(new AddAverageCCValueToGraphHeadMap(
        localCCAggregate.getAggregatePropertyKey(), vertexCountAggregate.getAggregatePropertyKey(),
        PROPERTY_KEY_AVERAGE));

    return graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graphHead, graph.getVertices(), graph.getEdges());
  }
}
