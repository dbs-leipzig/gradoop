/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.pagerank;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.labelpropagation.functions.EdgeToGellyEdgeMapper;
import org.gradoop.flink.algorithms.labelpropagation.functions.VertexToGellyVertexMapper;
import org.gradoop.flink.algorithms.pagerank.functions.PageRankToAttribute;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.link_analysis.PageRank}.
 */
public class PageRank implements UnaryGraphToGraphOperator {

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
    this.propertyKey = propertyKey;
    this.dampingFactor = dampingFactor;
    this.iterations = iterations;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    Graph gellyGraph = Graph.fromDataSet(
      graph.getVertices().map(new VertexToGellyVertexMapper(propertyKey)),
      graph.getEdges().map(new EdgeToGellyEdgeMapper()),
      graph.getConfig().getExecutionEnvironment());
    DataSet<Vertex> newVertices = new org.apache.flink.graph.library.link_analysis.PageRank<>(
      dampingFactor, iterations)
      .run(gellyGraph)
      .join(graph.getVertices())
      .where(0)
      .equalTo(new Id<>())
      .with(new PageRankToAttribute());
    return graph.getConfig().getLogicalGraphFactory().fromDataSets(newVertices, graph.getEdges());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return PageRank.class.getName();
  }
}
