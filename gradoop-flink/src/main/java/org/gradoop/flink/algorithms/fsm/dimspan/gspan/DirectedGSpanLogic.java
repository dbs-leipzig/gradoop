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
package org.gradoop.flink.algorithms.fsm.dimspan.gspan;


import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;

/**
 * Provides methods for logic related to the gSpan algorithm in directed mode.
 */
public class DirectedGSpanLogic extends GSpanLogicBase {

  /**
   * util methods to interpret and manipulate int-array encoded graphs
   */
  private final GraphUtils graphUtils = new GraphUtilsBase();

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public DirectedGSpanLogic(DIMSpanConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  protected boolean getSingleEdgePatternIsOutgoing(int[] graph, int edgeId, boolean loop) {
    return loop || graphUtils.isOutgoing(graph, edgeId);
  }

  @Override
  protected boolean getExtensionIsOutgoing(int[] graph, int edgeId, boolean fromFrom) {
    return fromFrom == graphUtils.isOutgoing(graph, edgeId);
  }

  @Override
  protected void storeSingleEdgePatternEmbeddings(PatternEmbeddingsMap patternEmbeddings,
    int[] pattern, int[] vertexIds, int[] edgeIds, int fromLabel, int toLabel, boolean loop) {

    patternEmbeddings.put(pattern, vertexIds, edgeIds);
  }

}
