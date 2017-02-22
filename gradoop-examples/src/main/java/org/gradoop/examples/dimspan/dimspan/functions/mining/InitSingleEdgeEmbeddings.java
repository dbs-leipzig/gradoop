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

package org.gradoop.examples.dimspan.dimspan.functions.mining;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConfig;
import org.gradoop.examples.dimspan.dimspan.config.DataflowStep;
import org.gradoop.examples.dimspan.dimspan.gspan.GSpanAlgorithm;
import org.gradoop.examples.dimspan.dimspan.representation.PatternEmbeddingsMap;
import org.gradoop.examples.dimspan.dimspan.representation.Simple16Compressor;
import org.gradoop.examples.dimspan.dimspan.tuples.GraphEmbeddingsPair;


/**
 * graph => (graph, 1-edge pattern -> embeddings)
 */
public class InitSingleEdgeEmbeddings implements MapFunction<int[], GraphEmbeddingsPair> {

  /**
   * pattern generation logic
   */
  private final GSpanAlgorithm gSpan;

  private final boolean graphCompressionEnabled;
  private final boolean patternCompressionEnabled;
  private final boolean embeddingCompressionEnabled;

  /**
   * Constructor.
   *
   * @param gSpan pattern generation logic
   * @param fsmConfig
   */
  public InitSingleEdgeEmbeddings(GSpanAlgorithm gSpan, DIMSpanConfig fsmConfig) {
    this.gSpan = gSpan;
    graphCompressionEnabled = fsmConfig.isGraphCompressionEnabled();
    embeddingCompressionEnabled = fsmConfig.isEmbeddingCompressionEnabled();
    patternCompressionEnabled =
      fsmConfig.getPatternCompressionInStep() == DataflowStep.MAP;
  }

  @Override
  public GraphEmbeddingsPair map(int[] graph) throws Exception {

    PatternEmbeddingsMap map = gSpan.getSingleEdgePatternEmbeddings(graph);

    if (graphCompressionEnabled) {
      graph = Simple16Compressor.compress(graph);
    }

    if (patternCompressionEnabled) {
      Simple16Compressor.compressPatterns(map);
    }

    if (embeddingCompressionEnabled) {
      Simple16Compressor.compressEmbeddings(map);
    }

    return new GraphEmbeddingsPair(graph, map);
  }

}
