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
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.functions.DecodeDFSCodes;
import org.gradoop.flink.algorithms.fsm.gspan.functions.EncodeTransactions;
import org.gradoop.flink.algorithms.fsm.gspan.functions.IterativeGSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator {

  public static final String SUPPORT_KEY = "support";
  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   *
   */
  public TransactionalFSM(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection execute(GraphCollection collection)  {
    GradoopFlinkConfig gradoopFlinkConfig = collection.getConfig();

    // create transactions from graph collection
    DataSet<GraphTransaction> transactions = collection
      .toTransactions()
      .getTransactions();

    // dictionary encoding
    DataSet<GSpanGraph> encodedTransactions = transactions
      .mapPartition(new EncodeTransactions(fsmConfig));

    // find frequent subgraphs
    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
      encodedTransactions.mapPartition(new IterativeGSpan(fsmConfig));

    // dictionary decoding
    transactions = frequentSubgraphs.mapPartition(new DecodeDFSCodes(
      fsmConfig,
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory())
    );

    return GraphCollection.fromTransactions(
      new GraphTransactions(transactions, gradoopFlinkConfig));
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
