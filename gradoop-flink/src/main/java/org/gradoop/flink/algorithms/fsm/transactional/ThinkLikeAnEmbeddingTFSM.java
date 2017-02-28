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

package org.gradoop.flink.algorithms.fsm.transactional;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.ThinkLikeAnEmbeddingFSMBase;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MinEdgeCount;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.IsResult;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.TFSMSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.TFSMSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.TFSMSubgraphOnly;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.ToTFSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.TFSMWrapInSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class ThinkLikeAnEmbeddingTFSM
  extends ThinkLikeAnEmbeddingFSMBase<TFSMGraph, TFSMSubgraph, TFSMSubgraphEmbeddings> {

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   *
   */
  public ThinkLikeAnEmbeddingTFSM(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  /**
   * Core mining method.
   *
   * @param transactions search space
   *
   * @return frequent subgraphs
   */
  public DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions) {

    transactions = preProcess(transactions);

    DataSet<TFSMGraph> graphs = transactions
      .map(new ToTFSMGraph());

    DataSet<TFSMSubgraphEmbeddings> embeddings = graphs
      .flatMap(new TFSMSingleEdgeEmbeddings(fsmConfig));

    // ITERATION HEAD
    IterativeDataSet<TFSMSubgraphEmbeddings> iterative = embeddings
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    // get frequent subgraphs
    DataSet<TFSMSubgraphEmbeddings> parentEmbeddings = iterative
      .filter(new IsResult<>(false));

    DataSet<TFSMSubgraph> frequentSubgraphs =
      getFrequentSubgraphs(parentEmbeddings);

    parentEmbeddings =
      filterByFrequentSubgraphs(parentEmbeddings, frequentSubgraphs);

    DataSet<TFSMSubgraphEmbeddings> childEmbeddings =
      growEmbeddingsOfFrequentSubgraphs(parentEmbeddings, frequentSubgraphs);

    DataSet<TFSMSubgraphEmbeddings> resultIncrement = frequentSubgraphs
      .map(new TFSMWrapInSubgraphEmbeddings());

    DataSet<TFSMSubgraphEmbeddings> resultAndEmbeddings = iterative
      .filter(new IsResult<>(true))
      .union(resultIncrement)
      .union(childEmbeddings);

    // ITERATION FOOTER

    DataSet<TFSMSubgraph>  allFrequentSubgraphs = iterative
      .closeWith(resultAndEmbeddings, childEmbeddings)
      .filter(new IsResult<>(true))
      .map(new TFSMSubgraphOnly());


    if (fsmConfig.getMinEdgeCount() > 1) {
      allFrequentSubgraphs = allFrequentSubgraphs
        .filter(new MinEdgeCount<>(fsmConfig));
    }

    return allFrequentSubgraphs.map(new TFSMSubgraphDecoder(gradoopFlinkConfig));
  }

  /**
   * Determines frequent subgraphs in a set of embeddings.
   *
   * @param embeddings set of embeddings
   * @return frequent subgraphs
   */
  private DataSet<TFSMSubgraph> getFrequentSubgraphs(
    DataSet<TFSMSubgraphEmbeddings> embeddings) {
    return embeddings
        .map(new TFSMSubgraphOnly())
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<>())
        .withBroadcastSet(minFrequency, DIMSpanConstants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
