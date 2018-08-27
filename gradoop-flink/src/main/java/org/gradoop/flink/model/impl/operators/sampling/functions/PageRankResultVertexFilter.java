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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.sampling.PageRankSampling;

/**
 * Retains all vertices with a PageRank-score greater or equal/smaller than a given
 * sampling threshold - depending on the Boolean set in {@code sampleGreaterThanThreshold}.
 */
public class PageRankResultVertexFilter implements FilterFunction<Vertex> {

  /**
   * Sampling threshold for PageRankScore
   */
  private final double threshold;
  /**
   * Whether to sample vertices with PageRank-score greater (true) or equal/smaller (false)
   * the threshold
   */
  private final boolean sampleGreaterThanThreshold;
  /**
   * Whether to sample all vertices (true) or none of them (false), in case all vertices got the
   * same PageRank-score.
   */
  private final boolean keepVerticesIfSameScore;

  /**
   * Creates a new filter instance.
   *
   * @param threshold The threshold for the PageRank-score (ranging between 0 and 1 when scaled),
   *                  determining if a vertex is sampled, e.g. 0.5
   * @param sampleGreaterThanThreshold Whether to sample vertices with PageRank-score
   *                                   greater (true) or equal/smaller (false) the threshold
   * @param keepVerticesIfSameScore Whether to sample all vertices (true) or none of them (false)
   *                                in case all vertices got the same PageRank-score.
   */
  public PageRankResultVertexFilter(double threshold, boolean sampleGreaterThanThreshold,
    boolean keepVerticesIfSameScore) {
    this.threshold = threshold;
    this.sampleGreaterThanThreshold = sampleGreaterThanThreshold;
    this.keepVerticesIfSameScore = keepVerticesIfSameScore;
  }

  @Override
  public boolean filter(Vertex v) throws Exception {
    if (v.hasProperty(PageRankSampling.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY)) {
      double pr = v.getPropertyValue(PageRankSampling.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY)
        .getDouble();
      if (sampleGreaterThanThreshold) {
        return pr > threshold;
      } else {
        return pr <= threshold;
      }
    } else {
      return keepVerticesIfSameScore;
    }
  }
}
