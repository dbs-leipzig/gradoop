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

package org.gradoop.flink.algorithms.fsm.dimspan.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphWithPatternEmbeddingsMap;


/**
 * graph => (graph, 1-edge pattern -> embeddings)
 */
public class InitSingleEdgePatternEmbeddingsMap implements MapFunction<int[], GraphWithPatternEmbeddingsMap> {

  /**
   * pattern generation logic
   */
  private final GSpanLogic gSpan;

  /**
   * flag to enable graph compression (true=enabled)
   */
  private final boolean compressGraphs;

  /**
   * flag to enable pattern compression (true=enabled)
   */
  private final boolean compressPatterns;

  /**
   * flag to enable embedding compression (true=enabled)
   */
  private final boolean compressEmbeddings;

  /**
   * Constructor.
   *
   * @param gSpan pattern generation logic
   * @param fsmConfig FSM configuration
   */
  public InitSingleEdgePatternEmbeddingsMap(GSpanLogic gSpan, DIMSpanConfig fsmConfig) {
    this.gSpan = gSpan;

    // set compression flags depending on configuration
    compressGraphs = fsmConfig.isGraphCompressionEnabled();
    compressEmbeddings = fsmConfig.isEmbeddingCompressionEnabled();
    compressPatterns =
      fsmConfig.getPatternCompressionInStep() == DataflowStep.MAP;
  }

  @Override
  public GraphWithPatternEmbeddingsMap map(int[] graph) throws Exception {

    PatternEmbeddingsMap map = gSpan.getSingleEdgePatternEmbeddings(graph);

    if (compressGraphs) {
      graph = Simple16Compressor.compress(graph);
    }

    if (compressPatterns) {
      Simple16Compressor.compressPatterns(map);
    }

    if (compressEmbeddings) {
      Simple16Compressor.compressEmbeddings(map);
    }

    return new GraphWithPatternEmbeddingsMap(graph, map);
  }

}
