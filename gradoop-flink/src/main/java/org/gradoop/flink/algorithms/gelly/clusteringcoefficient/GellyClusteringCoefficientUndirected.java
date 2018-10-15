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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient;
import org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions.LocalCCResultTupleToVertexJoin;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions.LocalUndirectedCCResultToTupleMap;
import org.gradoop.flink.algorithms.gelly.functions.WritePropertyToGraphHeadMap;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Gradoop EPGM model wrapper for Flink Gellys implementation of the clustering coefficient
 * algorithm for undirected graphs {@link org.apache.flink.graph.library.clustering.undirected}.
 * Returns the initial {@link LogicalGraph} with local values written to the vertices, average
 * and global value written to the graph head.
 */
public class GellyClusteringCoefficientUndirected extends ClusteringCoefficientBase {

  /**
   * Creates an instance of the GellyClusteringCoefficientUndirected wrapper class.
   * Calls constructor of super class {@link ClusteringCoefficientBase}.
   */
  public GellyClusteringCoefficientUndirected() {
    super();
  }

  /**
   * {@inheritDoc}
   *
   * Calls Flink Gelly algorithms to compute the local, average and global clustering coefficient
   * for an undirected graph.
   */
  @Override
  protected LogicalGraph executeInternal(Graph<GradoopId, NullValue, NullValue> gellyGraph)
    throws Exception {

    DataSet<Vertex> resultVertices = new org.apache.flink.graph.library.clustering.undirected
      .LocalClusteringCoefficient<GradoopId, NullValue, NullValue>().run(gellyGraph)
      .map(new LocalUndirectedCCResultToTupleMap())
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new LocalCCResultTupleToVertexJoin());

    AverageClusteringCoefficient average = new org.apache.flink.graph.library.clustering.undirected
      .AverageClusteringCoefficient<GradoopId, NullValue, NullValue>().run(gellyGraph);

    GlobalClusteringCoefficient global = new org.apache.flink.graph.library.clustering.undirected
      .GlobalClusteringCoefficient<GradoopId, NullValue, NullValue>().run(gellyGraph);

    currentGraph.getConfig().getExecutionEnvironment().execute();

    double averageValue = average.getResult().getAverageClusteringCoefficient();
    double globalValue = global.getResult().getGlobalClusteringCoefficientScore();

    DataSet<GraphHead> resultHead = currentGraph.getGraphHead()
      .map(new WritePropertyToGraphHeadMap(ClusteringCoefficientBase.PROPERTY_KEY_AVERAGE,
        PropertyValue.create(averageValue)))
      .map(new WritePropertyToGraphHeadMap(ClusteringCoefficientBase.PROPERTY_KEY_GLOBAL,
        PropertyValue.create(globalValue)));

    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(
      resultHead, resultVertices, currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return GellyClusteringCoefficientUndirected.class.getName();
  }
}
