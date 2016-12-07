/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

import java.util.Collection;

/**
 * (graph, k-edge pattern -> embeddings) => (graph, k+1-edge pattern -> embeddings)
 */
public class PatternGrowth extends RichMapFunction<GraphEmbeddingsPair, GraphEmbeddingsPair> {

  /**
   * k-edge frequent patterns
   */
  private Collection<TraversalCode<String>> frequentPatterns;
  /**
   * pattern growth logic
   */
  private final GSpanKernel gSpan;

  /**
   * Constructor.
   *
   * @param gSpan pattern growth logic
   */
  public PatternGrowth(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentPatterns = getRuntimeContext().getBroadcastVariable(TFSMConstants.FREQUENT_PATTERNS);
  }

  @Override
  public GraphEmbeddingsPair map(GraphEmbeddingsPair graphEmbeddingsPair) throws Exception {

    if (graphEmbeddingsPair.getAdjacencyList().getOutgoingRows().isEmpty()) {
      for (TraversalCode<String> code : frequentPatterns) {
        graphEmbeddingsPair.getPatternEmbeddings().put(code, null);
      }
    } else {
      gSpan.growChildren(graphEmbeddingsPair, frequentPatterns);
    }

    return graphEmbeddingsPair;
  }
}
