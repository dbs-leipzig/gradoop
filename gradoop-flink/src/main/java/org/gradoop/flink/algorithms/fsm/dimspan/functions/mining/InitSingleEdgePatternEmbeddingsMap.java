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
