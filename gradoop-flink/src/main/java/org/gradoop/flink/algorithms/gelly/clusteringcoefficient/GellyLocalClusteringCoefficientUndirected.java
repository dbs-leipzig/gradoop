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
package org.gradoop.flink.algorithms.gelly.clusteringcoefficient;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions.LocalCCResultTupleToVertexJoin;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.functions.LocalUndirectedCCResultToTupleMap;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Gradoop EPGM model wrapper for Flink Gellys implementation of the local clustering coefficient
 * algorithm for undirected graphs
 * {@link org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient}.
 * Returns the initial {@link BaseGraph} with local values written to the vertices.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class GellyLocalClusteringCoefficientUndirected<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends ClusteringCoefficientBase<G, V, E, LG, GC> {

  /**
   * Creates an instance of the GellyLocalClusteringCoefficientUndirected wrapper class.
   * Calls constructor of super class {@link ClusteringCoefficientBase}.
   */
  public GellyLocalClusteringCoefficientUndirected() {
    super();
  }

  @Override
  protected LG executeInternal(Graph<GradoopId, NullValue, NullValue> gellyGraph) throws Exception {

    DataSet<V> resultVertices = new org.apache.flink.graph.library.clustering.undirected
      .LocalClusteringCoefficient<GradoopId, NullValue, NullValue>().run(gellyGraph)
      .map(new LocalUndirectedCCResultToTupleMap())
      .join(currentGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new LocalCCResultTupleToVertexJoin<>());

    return currentGraph.getFactory().fromDataSets(
      currentGraph.getGraphHead(), resultVertices, currentGraph.getEdges());
  }
}
