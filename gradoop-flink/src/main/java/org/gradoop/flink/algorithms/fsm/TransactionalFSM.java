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
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.functions.ByFrequency;
import org.gradoop.flink.algorithms.fsm.functions.JoinSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm.functions.SingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.functions.CountableFrequentSubgraph;


import org.gradoop.flink.algorithms.fsm.functions.FrequentSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.functions.JoinMultiEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.tuples.FrequentSubgraph;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;
  private DataSet<Long> minFrequency;

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

    transactions = execute(transactions);

    return GraphCollection.fromTransactions(
      new GraphTransactions(transactions, gradoopFlinkConfig));
  }

  public DataSet<GraphTransaction> execute(
    DataSet<GraphTransaction> transactions) {
    minFrequency = Count
      .count(transactions)
      .map(new MinFrequency(fsmConfig));

    DataSet<SubgraphEmbeddings>
      embeddings = transactions
      .flatMap(new SingleEdgeEmbeddings(fsmConfig));

    DataSet<FrequentSubgraph> frequentSubgraphs =
      getFrequentSubgraphs(embeddings);

    DataSet<FrequentSubgraph> allFrequentSubgraphs =
      frequentSubgraphs;

    embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

    embeddings = embeddings
      .groupBy(0)
      .reduceGroup(new JoinSingleEdgeEmbeddings(fsmConfig));

    frequentSubgraphs = getFrequentSubgraphs(embeddings);
    allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);

    for (int i = 0; i < 3; i++) {
      embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

      embeddings = embeddings
        .groupBy(0, 1)
        .reduceGroup(new JoinMultiEdgeEmbeddings(fsmConfig));

      frequentSubgraphs = getFrequentSubgraphs(embeddings);
      allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);
    }

    transactions = allFrequentSubgraphs
      .map(new FrequentSubgraphDecoder());
    return transactions;
  }

  private DataSet<SubgraphEmbeddings> filterByFrequentSubgraphs(
    DataSet<SubgraphEmbeddings> embeddings,
    DataSet<FrequentSubgraph> frequentSubgraphs) {

    return embeddings
      .join(frequentSubgraphs)
      .where(2).equalTo(0)
      .with(new LeftSide<SubgraphEmbeddings, FrequentSubgraph>());
  }

  private DataSet<FrequentSubgraph> getFrequentSubgraphs(
    DataSet<SubgraphEmbeddings> embeddings) {
    return embeddings
        .map(new CountableFrequentSubgraph())
        .groupBy(0)
        .sum(1)
        .filter(new ByFrequency())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
