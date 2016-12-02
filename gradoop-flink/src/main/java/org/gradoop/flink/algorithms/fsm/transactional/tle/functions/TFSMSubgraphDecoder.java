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

package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;

import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * FSM subgraph -> Gradoop graph transaction.
 */
public class TFSMSubgraphDecoder extends SubgraphDecoder
  implements MapFunction<TFSMSubgraph, GraphTransaction> {

  /**
   * Label of frequent subgraphs.
   */
  private static final String SUBGRAPH_LABEL = "FrequentSubgraph";

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   */
  public TFSMSubgraphDecoder(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphTransaction map(TFSMSubgraph value) throws Exception {
    return createTransaction(value, SUBGRAPH_LABEL);
  }

}
