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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.gelly.functions.WritePropertyToGraphHeadMap;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Gradoop EPGM model wrapper for Flink Gellys implementation of the global clustering coefficient
 * algorithm for directed graphs
 * {@link org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient}.
 * Returns the initial {@link LogicalGraph} with global value written to the graph head.
 */
public class GellyGlobalClusteringCoefficientDirected extends ClusteringCoefficientBase {

  /**
   * Creates an instance of the GellyGlobalClusteringCoefficientDirected wrapper class.
   * Calls constructor of super class {@link ClusteringCoefficientBase}.
   */
  public GellyGlobalClusteringCoefficientDirected() {
    super();
  }

  /**
   * {@inheritDoc}
   *
   * Calls Flink Gelly algorithms to compute the global clustering coefficient for a directed graph.
   */
  @Override
  protected LogicalGraph executeInternal(Graph<GradoopId, NullValue, NullValue> gellyGraph)
    throws Exception {

    GlobalClusteringCoefficient global = new org.apache.flink.graph.library.clustering.directed
      .GlobalClusteringCoefficient<GradoopId, NullValue, NullValue>().run(gellyGraph);

    currentGraph.getConfig().getExecutionEnvironment().execute();

    double globalValue = global.getResult().getGlobalClusteringCoefficientScore();

    DataSet<GraphHead> resultHead = currentGraph.getGraphHead()
      .map(new WritePropertyToGraphHeadMap(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL,
        PropertyValue.create(globalValue)));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(
      resultHead, currentGraph.getVertices(), currentGraph.getEdges());
  }
}
