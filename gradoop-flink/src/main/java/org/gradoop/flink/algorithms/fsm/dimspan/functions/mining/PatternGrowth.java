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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.dimspan.gspan.GSpanAlgorithm;
import org.gradoop.flink.algorithms.fsm.dimspan.model.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.GraphEmbeddingsPair;

import java.util.List;

/**
 * (graph, k-edge pattern -> embeddings) => (graph, k+1-edge pattern -> embeddings)
 */
public class PatternGrowth extends RichMapFunction<GraphEmbeddingsPair, GraphEmbeddingsPair> {

  /**
   * compressed k-edge frequent patterns for fast embedding map lookup
   */
  private List<int[]> compressedFrequentPatterns;

  /**
   * uncompressed k-edge frequent patterns for pattern growth
   */
  private List<int[]> frequentPatterns;

  /**
   * list of rightmost paths, index relates to frequent patterns
   */
  private List<int[]> rightmostPaths;

  /**
   * pattern growth logic (directed or undirected mode)
   */
  private final GSpanAlgorithm gSpan;

  /**
   * flag to enable graph compression (true=enabled)
   */
  private final boolean compressGraphs;

  /**
   * flag to enable pattern compression (true=enabled)
   */
  private final boolean compressPatterns;

  /**
   * flag to enable pattern decompression (true=enabled)
   */
  private final boolean uncompressFrequentPatterns;

  /**
   * flag to enable embedding compression (true=enabled)
   */
  private final boolean compressEmbeddings;

  /**
   * flag to enable pattern verification before counting (true=enabled)
   */
  private final boolean validatePatterns;

  /**
   * Constructor.
   *
   * @param gSpan pattern growth logic
   * @param fsmConfig FSM Configuration
   */
  public PatternGrowth(GSpanAlgorithm gSpan, DIMSpanConfig fsmConfig) {

    // set pattern growth logic for directed or undirected mode
    this.gSpan = gSpan;

    // cache compression flags
    compressGraphs = fsmConfig.isGraphCompressionEnabled();
    compressEmbeddings = fsmConfig.isEmbeddingCompressionEnabled();
    compressPatterns = fsmConfig.getPatternCompressionInStep() == DataflowStep.MAP;
    uncompressFrequentPatterns = fsmConfig.getPatternCompressionInStep() != DataflowStep.WITHOUT;

    // cache validation flag
    validatePatterns = fsmConfig.getPatternValidationInStep() == DataflowStep.MAP;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // broadcast reception

    List<int[]> broadcast =
      getRuntimeContext().getBroadcastVariable(DIMSpanConstants.FREQUENT_PATTERNS);

    int patternCount = broadcast.size();

    this.frequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (int[] pattern : broadcast) {
      // uncompress
      if (uncompressFrequentPatterns) {
        pattern = Simple16Compressor.uncompress(pattern);
      }
      frequentPatterns.add(pattern);
    }

    // sort
    frequentPatterns.sort(new DFSCodeComparator());

    // calculate rightmost paths
    this.rightmostPaths = Lists.newArrayListWithExpectedSize(patternCount);
    this.compressedFrequentPatterns = Lists.newArrayListWithExpectedSize(patternCount);

    for (int[] pattern : frequentPatterns) {
      rightmostPaths.add(gSpan.getRightmostPathTimes(pattern));

      // TODO: directly store compressed patterns at reception
      compressedFrequentPatterns
        .add(compressPatterns ? Simple16Compressor.compress(pattern) : pattern);
    }
  }

  @Override
  public GraphEmbeddingsPair map(GraphEmbeddingsPair pair) throws Exception {

    // union k-1 edge frequent patterns with k-edge ones
    if (pair.isCollector()) {
      for (int[] pattern : frequentPatterns) {
        pair.getPatternEmbeddings().collect(pattern);
      }
    } else {
      int[] graph = pair.getGraph();

      // uncompress graph
      if (compressGraphs) {
        graph = Simple16Compressor.uncompress(graph);
      }

      // execute pattern growth for all supported frequent patterns
      PatternEmbeddingsMap childMap = gSpan.growPatterns(graph, pair.getPatternEmbeddings(),
        frequentPatterns, rightmostPaths, compressEmbeddings, compressedFrequentPatterns);

      // drop non-minimal patterns if configured to be executed here
      if (validatePatterns){
        PatternEmbeddingsMap validatedMap = PatternEmbeddingsMap.getEmptyOne();

        for (int i = 0; i < childMap.getPatternCount(); i++) {
          int[] pattern = childMap.getPattern(i);

          if (gSpan.isMinimal(pattern)) {
            int[] embeddingData = childMap.getEmbeddingData()[i];
            validatedMap.put(pattern, embeddingData);
          }
        }

        childMap = validatedMap;
      }

      // update pattern-embedding map
      pair.setPatternEmbeddings(childMap);

      // compress patterns and embedding, if configured
      // NOTE: graphs will remain compressed
      if (compressPatterns) {
        Simple16Compressor.compressPatterns(pair.getPatternEmbeddings());
      }

      if (compressEmbeddings) {
        Simple16Compressor.compressEmbeddings(pair.getPatternEmbeddings());
      }
    }

    return pair;
  }
}
