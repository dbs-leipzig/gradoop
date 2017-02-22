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
