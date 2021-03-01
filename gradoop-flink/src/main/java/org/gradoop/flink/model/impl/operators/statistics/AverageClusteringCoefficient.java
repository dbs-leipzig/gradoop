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

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.ClusteringCoefficientBase;
import org.gradoop.flink.algorithms.gelly.clusteringcoefficient.GellyLocalClusteringCoefficientDirected;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.functions.average.AverageVertexProperty;

/**
 * Calculates the average local clustering coefficient of a graph and writes it to the graph head.
 * Uses the Gradoop EPGM model wrapper for Flink Gellys implementation of the local clustering
 * coefficient algorithm for directed graphs {@link GellyLocalClusteringCoefficientDirected}
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class AverageClusteringCoefficient<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Property key to access the average clustering coefficient value stored the graph head
   */
  public static final String PROPERTY_KEY_AVERAGE = "clustering_coefficient_average";

  @Override
  public LG execute(LG graph) {

    return graph
      .callForGraph(new GellyLocalClusteringCoefficientDirected<>())
      .aggregate(new AverageVertexProperty(ClusteringCoefficientBase.PROPERTY_KEY_LOCAL,
        PROPERTY_KEY_AVERAGE));
  }
}
