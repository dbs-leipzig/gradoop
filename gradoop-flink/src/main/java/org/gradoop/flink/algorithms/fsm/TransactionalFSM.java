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

package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.EPGMGraphTransactionToLabeledGraph;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Gradoop operator wrapping the DIMSpan algorithm for transactional frequent subgraph mining.
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator {

  /**
   * Flink-based implementation of the DIMSpan algorithm
   */
  private final DIMSpan dimSpan;

  /**
   * Constructor.
   *
   * @param minSupport minimum support threshold.
   */
  public TransactionalFSM(float minSupport) {
    // only directed mode for use withing Gradoop programs as EPGM is directed
    DIMSpanConfig fsmConfig = new DIMSpanConfig(minSupport, true);
    dimSpan = new DIMSpan(fsmConfig);
  }

  /**
   * Unit testing constructor.
   *
   * @param fsmConfig externally configured DIMSpan configuration
   */
  public TransactionalFSM(DIMSpanConfig fsmConfig) {
    dimSpan = new DIMSpan(fsmConfig);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // convert Gradoop graph collection to DIMSpan input format
    DataSet<LabeledGraphStringString> input = collection
      .toTransactions()
      .getTransactions()
      .map(new EPGMGraphTransactionToLabeledGraph());

    // run DIMSpan
    DataSet<GraphTransaction> output = dimSpan.execute(input);

    // convert to Gradoop graph collection
    return GraphCollection
      .fromTransactions(new GraphTransactions(output, collection.getConfig()));
  }

  @Override
  public String getName() {
    return DIMSpan.class.getSimpleName();
  }
}
