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

package org.gradoop.flink.algorithms.fsm.transactional.gspan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.ToUndirectedAdjacencyList;
import org.gradoop.flink.algorithms.fsm.transactional.tle.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.DirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.UndirectedGSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.ToDirectedAdjacencyList;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.ToGraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.functions.Validate;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * Superclass of gSpan implementations.
 */
public abstract class GSpanBase extends TransactionalFSMBase {

  /**
   * mining logic
   */
  protected final GSpanKernel gSpan;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public GSpanBase(FSMConfig fsmConfig) {
    super(fsmConfig);
    gSpan = fsmConfig.isDirected() ? new DirectedGSpanKernel() : new UndirectedGSpanKernel();
  }

  /**
   * Encodes the search space before executing FSM.
   *
   * @param transactions search space as Gradoop graph transactions
   *
   * @return frequent patterns as Gradoop graph transactions
   */
  @Override
  public DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions) {

    transactions = preProcess(transactions);

    DataSet<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> adjacencyLists =
      transactions
        .map(fsmConfig.isDirected() ?
          new ToDirectedAdjacencyList() : new ToUndirectedAdjacencyList());

    DataSet<TraversalCode<String>> allFrequentPatterns = mine(adjacencyLists);

    return allFrequentPatterns
      .map(new ToGraphTransaction());
  }

  /**
   * Executes the actual mining process.
   *
   * @param graphs search space
   * @return frequent patterns
   */
  protected abstract DataSet<TraversalCode<String>> mine(
    DataSet<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> graphs);

  /**
   * Identifies valid frequent patterns from a dataset of reported patterns.
   *
   * @param patterns reported patterns
   * @return valid frequent patterns
   */
  protected DataSet<TraversalCode<String>> getFrequentPatterns(
    FlatMapOperator<GraphEmbeddingsPair, WithCount<TraversalCode<String>>> patterns) {
    return patterns
        .groupBy(0)
        .combineGroup(sumPartition())
        .filter(new Validate(gSpan))
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<>())
        .withBroadcastSet(minFrequency, TFSMConstants.MIN_FREQUENCY)
        .map(new ValueOfWithCount<>());
  }
}
