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

package org.gradoop.flink.algorithms.fsm.transactional.tle.ccs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.transactional.tle.ccs.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.functions.SubgraphDecoder;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * FSM subgraph -> Gradoop graph transaction.
 */
public class CCSSubgraphDecoder extends SubgraphDecoder
  implements MapFunction<CCSSubgraph, GraphTransaction> {

  /**
   * Label of frequent subgraphs.
   */
  private static final String SUBGRAPH_LABEL = "CharacteristicSubgraph";

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   */
  public CCSSubgraphDecoder(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphTransaction map(CCSSubgraph value) throws Exception {
    GraphTransaction transaction = createTransaction(value, SUBGRAPH_LABEL);

    transaction.getGraphHead().setProperty(
      CategoryCharacteristicSubgraphs.CATEGORY_KEY,
      value.getCategory()
    );

    return transaction;
  }

}
