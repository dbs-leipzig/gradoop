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
package org.gradoop.flink.algorithms.gelly.pagerank;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.pagerank.functions.PageRankToAttribute;
import org.gradoop.flink.algorithms.gelly.pagerank.functions.PageRankResultKey;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.linkanalysis.PageRank}.
 */
public class PageRank extends GellyAlgorithm<NullValue, NullValue> {

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
   * Constructor for Page Rank with fixed number of iterations.
   *
   * @param propertyKey   Property key to store the rank in.
   * @param dampingFactor Damping factor.
   * @param iterations    Number of iterations.
   */
  public PageRank(String propertyKey, double dampingFactor, int iterations) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.propertyKey = propertyKey;
    this.dampingFactor = dampingFactor;
    this.iterations = iterations;
  }

  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph)
    throws Exception {
    DataSet<Vertex> newVertices =
      new org.apache.flink.graph.library.linkanalysis.PageRank<GradoopId, NullValue, NullValue>(
        dampingFactor, iterations)
      .run(graph)
      .join(currentGraph.getVertices())
      .where(new PageRankResultKey())
      .equalTo(new Id<>())
      .with(new PageRankToAttribute(propertyKey));
    return currentGraph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices,
      currentGraph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return PageRank.class.getName();
  }
}
