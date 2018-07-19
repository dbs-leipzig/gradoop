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
package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.EPGMGraphTransactionToLabeledGraph;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

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
      .getGraphTransactions()
      .map(new EPGMGraphTransactionToLabeledGraph());

    // run DIMSpan
    DataSet<GraphTransaction> output = dimSpan.execute(input);

    // convert to Gradoop graph collection
    return collection.getConfig().getGraphCollectionFactory().fromTransactions(output);
  }

  @Override
  public String getName() {
    return DIMSpan.class.getSimpleName();
  }
}
