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
package org.gradoop.flink.model.impl.operators.sampling;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.pagerank.PageRank;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.model.impl.operators.sampling.functions.AddPageRankScoresToVertexCrossFunction;
import org.gradoop.flink.model.impl.operators.sampling.functions.PageRankResultVertexFilter;

/**
 * Computes a PageRank-Sampling of the graph.
 *
 * Uses the Gradoop-Wrapper of Flinks PageRank-algorithm {@link PageRank} with a dampening factor
 * and a number of maximum iterations. It computes a per-vertex score which is the sum of the
 * PageRank-scores transmitted over all in-edges. The score of each vertex is divided evenly
 * among its out-edges.
 *
 * If vertices got different PageRank-scores, all scores are scaled in a range between 0 and 1.
 * Then it retains all vertices with a PageRank-score greater or equal/smaller than a given
 * sampling threshold - depending on the Boolean set in {@code sampleGreaterThanThreshold}.
 *
 * If ALL vertices got the same PageRank-score, it can be decided whether to sample all vertices
 * or none of them - depending on the Boolean set in {@code keepVerticesIfSameScore}.
 *
 * Retains all edges which source- and target-vertices were chosen. There may retain some
 * unconnected vertices in the sampled graph.
 */
public class PageRankSampling extends SamplingAlgorithm {

  /**
   * Dampening factor used by PageRank-algorithm
   */
  private final double dampeningFactor;
  /**
   * Number of iterations used by PageRank-algorithm
   */
  private final int maxIteration;
  /**
   * Sampling threshold for PageRankScore
   */
  private final double threshold;
  /**
   * Whether to sample vertices with PageRank-score greater (true) or equal/smaller (false)
   * than the threshold
   */
  private final boolean sampleGreaterThanThreshold;
  /**
   * Whether to sample all vertices (true) or none of them (false), in case all vertices got the
   * same PageRank-score.
   */
  private final boolean keepVerticesIfSameScore;

  /**
   * Creates a new PageRankSampling instance.
   *
   * @param dampeningFactor The dampening factor used by PageRank-algorithm, e.g. 0.85
   * @param maxIteration The number of iterations used by PageRank-algorithm, e.g. 40
   * @param threshold The threshold for the PageRank-score (ranging between 0 and 1 when scaled),
   *                  determining if a vertex is sampled, e.g. 0.5
   * @param sampleGreaterThanThreshold Whether to sample vertices with a PageRank-score
   *                                   greater (true) or equal/smaller (false) the threshold
   * @param keepVerticesIfSameScore Whether to sample all vertices (true) or none of them (false)
   *                                in case all vertices got the same PageRank-score.
   */
  public PageRankSampling(double dampeningFactor, int maxIteration, double threshold,
    boolean sampleGreaterThanThreshold, boolean keepVerticesIfSameScore) {
    this.dampeningFactor = dampeningFactor;
    this.threshold = threshold;
    this.maxIteration = maxIteration;
    this.sampleGreaterThanThreshold = sampleGreaterThanThreshold;
    this.keepVerticesIfSameScore = keepVerticesIfSameScore;
  }

  /**
   * {@inheritDoc}
   *
   * Vertices are sampled by using the Gradoop-Wrapper of Flinks PageRank-algorithm
   * {@link PageRank}. If they got different PageRank-scores, all scores are scaled
   * in a range between 0 and 1.
   * Then all vertices with a PageRank-score greater or equal/smaller than a given sampling
   * threshold are retained - depending on the Boolean set in {@code sampleGreaterThanThreshold}.
   * If ALL vertices got the same PageRank-score, it can be decided whether to sample all
   * vertices or none of them - depending on the Boolean set in {@code keepVerticesIfSameScore}.
   * Retains all edges which source- and target-vertices were chosen. There may retain some
   * unconnected vertices in the sampled graph.
   */
  @Override
  public LogicalGraph sample(LogicalGraph graph) {

    LogicalGraph pageRankGraph = new PageRank(
      PAGE_RANK_SCORE_PROPERTY_KEY, dampeningFactor, maxIteration).execute(graph);

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graph.getGraphHead(), pageRankGraph.getVertices(), pageRankGraph.getEdges());

    graph = graph
      .aggregate(new MinVertexProperty(PAGE_RANK_SCORE_PROPERTY_KEY),
        new MaxVertexProperty(PAGE_RANK_SCORE_PROPERTY_KEY),
        new SumVertexProperty(PAGE_RANK_SCORE_PROPERTY_KEY),
        new VertexCount());

    DataSet<Vertex> scaledVertices = graph.getVertices()
      .crossWithTiny(graph.getGraphHead().first(1))
      .with(new AddPageRankScoresToVertexCrossFunction())
      .filter(new PageRankResultVertexFilter(
        threshold, sampleGreaterThanThreshold, keepVerticesIfSameScore));

    DataSet<Edge> newEdges = graph.getEdges()
      .join(scaledVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(scaledVertices)
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    graph = graph.getConfig().getLogicalGraphFactory().fromDataSets(
      graph.getGraphHead(), scaledVertices, newEdges);

    return graph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return PageRankSampling.class.getName();
  }
}
