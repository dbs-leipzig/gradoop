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

import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Base class for Gradoop EPGM model wrapper for Flink Gellys implementation of the clustering
 * coefficient algorithm. Implementations compute the local, average and global clustering
 * coefficient of a graph, where:
 * <pre>
 *   local - connectivity of a single vertex regarding its neighbors with value between 0.0 and 1.0
 *   average - mean over all local values
 *   global - connectivity of the graph as triangle to triplet ratio with value between 0.0 and 1.0
 * </pre>
 */
public abstract class ClusteringCoefficientBase extends GellyAlgorithm<NullValue, NullValue> {

  /**
   * Property key to access the local clustering coefficient value stored in the vertices
   */
  public static final String PROPERTY_KEY_LOCAL = "clustering_coefficient_local";

  /**
   * Property key to access the average clustering coefficient value stored the graph head
   */
  public static final String PROPERTY_KEY_AVERAGE = "clustering_coefficient_average";

  /**
   * Property key to access the global clustering coefficient value stored the graph head
   */
  public static final String PROPERTY_KEY_GLOBAL = "clustering_coefficient_global";

  /**
   * Constructor
   */
  public ClusteringCoefficientBase() {
    super(new VertexToGellyVertexWithNullValue(),
      new EdgeToGellyEdgeWithNullValue());
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph) throws
    Exception {
    return executeInternal(graph);
  }

  /**
   * Executes the computation of the clustering coefficient.
   *
   * @param gellyGraph Gelly graph with initialized vertices
   * @return {@link LogicalGraph} with local values written to the vertices, average and global
   * value written to the graph head
   * @throws Exception Thrown if the gelly algorithm fails
   */
  protected abstract LogicalGraph executeInternal(Graph<GradoopId, NullValue, NullValue> gellyGraph)
    throws Exception;

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return ClusteringCoefficientBase.class.getName();
  }
}
