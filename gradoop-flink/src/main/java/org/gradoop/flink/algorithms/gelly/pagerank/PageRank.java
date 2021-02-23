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
package org.gradoop.flink.algorithms.gelly.pagerank;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.pagerank.functions.PageRankResultKey;
import org.gradoop.flink.algorithms.gelly.pagerank.functions.PageRankToAttribute;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.linkanalysis.PageRank}.
 *
 * @param <G>  Gradoop graph head type.
 * @param <V>  Gradoop vertex type.
 * @param <E>  Gradoop edge type.
 * @param <LG> Gradoop type of the graph.
 * @param <GC> Gradoop type of the graph collection.
 */
public class PageRank<
  G extends GraphHead,
  V extends org.gradoop.common.model.api.entities.Vertex,
  E extends org.gradoop.common.model.api.entities.Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  extends GradoopGellyAlgorithm<G, V, E, LG, GC, NullValue, NullValue> {

  /**
   * Property key to store the page rank in.
   */
  private final String propertyKey;

  /**
   * Damping factor.
   */
  private final double dampingFactor;

  /**
   * Number of iterations.
   */
  private final int iterations;

  /**
   * Whether to include "zero-degree" vertices in the PageRank computation and result. These
   * vertices only affect the scores of other vertices indirectly through influencing the initial
   * proportional score of {@code (1 - damping factor) / number of vertices}.
   * If set to {@code false}, these vertices will NOT be part of the computation and the returned
   * result graph.
   */
  private final boolean includeZeroDegrees;

  /**
   * Constructor for Page Rank with fixed number of iterations and {@link #includeZeroDegrees}
   * set to {@code false}.
   *
   * @param propertyKey Property key to store the page rank in.
   * @param dampingFactor Damping factor.
   * @param iterations    Number of iterations.
   */
  public PageRank(String propertyKey, double dampingFactor, int iterations) {
    this(propertyKey, dampingFactor, iterations, false);
  }

  /**
   * Constructor for Page Rank with fixed number of iterations.
   *
   * @param propertyKey Property key to store the page rank in.
   * @param dampingFactor Damping factor.
   * @param iterations    Number of iterations.
   * @param includeZeroDegrees Whether to include "zero-degree" vertices in the PageRank
   *                                  computation and result.
   */
  public PageRank(String propertyKey, double dampingFactor, int iterations, boolean includeZeroDegrees) {
    super(new VertexToGellyVertexWithNullValue<>(), new EdgeToGellyEdgeWithNullValue<>());
    this.propertyKey = propertyKey;
    this.dampingFactor = dampingFactor;
    this.iterations = iterations;
    this.includeZeroDegrees = includeZeroDegrees;
  }

  @Override
  public LG executeInGelly(Graph<GradoopId, NullValue, NullValue> gellyGraph) throws Exception {
    DataSet<V> newVertices =
      new org.apache.flink.graph.library.linkanalysis.PageRank<GradoopId, NullValue, NullValue>(
        dampingFactor, iterations).setIncludeZeroDegreeVertices(includeZeroDegrees).run(gellyGraph)
      .join(currentGraph.getVertices())
      .where(new PageRankResultKey()).equalTo(new Id<>())
      .with(new PageRankToAttribute<>(propertyKey));
    return currentGraph.getFactory().fromDataSets(
      currentGraph.getGraphHead(), newVertices, currentGraph.getEdges());
  }
}
