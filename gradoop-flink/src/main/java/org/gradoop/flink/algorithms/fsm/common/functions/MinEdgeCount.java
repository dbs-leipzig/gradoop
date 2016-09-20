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

package org.gradoop.flink.algorithms.fsm.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.tuples.Subgraph;

/**
 * Filters a subgraph by minimum edge count.
 *
 * @param <S> subgraph type
 */
public class MinEdgeCount<S extends Subgraph> implements FilterFunction<S> {

  /**
   * Minimum number of edges
   */
  private final int minEdgeCount;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public MinEdgeCount(FSMConfig fsmConfig) {
    this.minEdgeCount = fsmConfig.getMinEdgeCount();
  }

  @Override
  public boolean filter(S subgraph) throws Exception {
    return subgraph.getEmbedding().getEdges().size() >= minEdgeCount;
  }
}
