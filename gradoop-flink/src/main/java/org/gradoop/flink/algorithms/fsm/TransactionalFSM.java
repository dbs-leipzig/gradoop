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
import org.gradoop.flink.algorithms.fsm.gspan.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.gspan.functions.JoinEmbeddings;
import org.gradoop.flink.algorithms.fsm.gspan.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm.gspan.functions.SingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.gspan.functions.SubgraphWithCount;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.utils.AddCount;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;
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

    minFrequency = Count
      .count(transactions)
      .map(new MinFrequency(fsmConfig));

    DataSet<CodeEmbeddings>
      embeddings = transactions
      .flatMap(new SingleEdgeEmbeddings());

    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs =
      getFrequentSubgraphs(embeddings);

    DataSet<WithCount<CompressedDFSCode>> allFrequentSubgraphs =
      frequentSubgraphs;

    embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

    embeddings = embeddings
      .groupBy(0)
      .reduceGroup(new JoinEmbeddings(1, 1));

    frequentSubgraphs = getFrequentSubgraphs(embeddings);
    allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);
    embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

    embeddings = embeddings
      .groupBy(0)
      .reduceGroup(new JoinEmbeddings(2, 2));

    try {
      embeddings.print();
    } catch (Exception e) {
      e.printStackTrace();
    }


    return null;
  }

  private DataSet<CodeEmbeddings> filterByFrequentSubgraphs(
    DataSet<CodeEmbeddings>
      embeddings,
    DataSet<WithCount<CompressedDFSCode>> frequentSubgraphs) {
    embeddings = embeddings
      .join(frequentSubgraphs)
      .where(1).equalTo(0)
      .with(new LeftSide<CodeEmbeddings,
          WithCount<CompressedDFSCode>>());
    return embeddings;
  }

  private DataSet<WithCount<CompressedDFSCode>> getFrequentSubgraphs(
    DataSet<CodeEmbeddings>
      embeddings) {
    return embeddings
        .map(new SubgraphWithCount())
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<CompressedDFSCode>())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
