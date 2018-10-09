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

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;

/**
 * Writes the PageRank-scores stored in the graphHead to all vertices.
 */
public class AddPageRankScoresToVertexCrossFunction
  implements CrossFunction<Vertex, GraphHead, Vertex> {

  /**
   * Writes the PageRank-scores stored in the graphHead to all vertices.
   */
  @Override
  public Vertex cross(Vertex vertex, GraphHead graphHead) {
    double min = graphHead.getPropertyValue(
      SamplingAlgorithm.MIN_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
    double max = graphHead.getPropertyValue(
      SamplingAlgorithm.MAX_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
    double sum = graphHead.getPropertyValue(
      SamplingAlgorithm.SUM_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();

    vertex.setProperty(SamplingAlgorithm.MIN_PAGE_RANK_SCORE_PROPERTY_KEY, min);
    vertex.setProperty(SamplingAlgorithm.MAX_PAGE_RANK_SCORE_PROPERTY_KEY, max);
    vertex.setProperty(SamplingAlgorithm.SUM_PAGE_RANK_SCORE_PROPERTY_KEY, sum);
    vertex.setProperty("vertexCount", graphHead.getPropertyValue("vertexCount"));

    if (min != max) {
      double score = vertex.getPropertyValue(
        SamplingAlgorithm.PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
      vertex.setProperty(
        SamplingAlgorithm.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY, (score - min) / (max - min));
    }

    return vertex;
  }
}
