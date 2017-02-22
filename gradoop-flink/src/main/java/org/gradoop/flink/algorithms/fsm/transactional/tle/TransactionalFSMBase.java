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

package org.gradoop.flink.algorithms.fsm.transactional.tle;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.DropPropertiesAndGraphContainment;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.FilterEdgesByLabel;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.FilterVerticesByLabel;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.NotEmpty;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MinFrequency;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Superclass of frequent subgraph implementations in the graph transaction setting.
 */
public abstract class TransactionalFSMBase implements UnaryCollectionToCollectionOperator {

  /**
   * FSM configuration
   */
  protected final FSMConfig fsmConfig;

  /**
   * search space size
   */
  protected DataSet<Long> graphCount;

  /**
   * minimum frequency for patterns to be considered to be frequent
   */
  protected DataSet<Long> minFrequency;

  /**
   * Gradoop configuration
   */
  protected GradoopFlinkConfig gradoopFlinkConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public TransactionalFSMBase(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public GraphCollection execute(GraphCollection collection)  {
    gradoopFlinkConfig = collection.getConfig();

    DataSet<GraphTransaction> input = collection
      .toTransactions()
      .getTransactions();

    DataSet<GraphTransaction> output = execute(input);

    return GraphCollection.fromTransactions(new GraphTransactions(output, gradoopFlinkConfig));
  }

  /**
   * Executes the algorithm for graphs in Gradoop transactional representation.
   *
   * @param transactions graphs in transactional representation
   *
   * @return frequent patterns in transactional representation
   */
  public GraphTransactions execute(GraphTransactions transactions) {
    this.gradoopFlinkConfig = transactions.getConfig();

    DataSet<GraphTransaction> input = transactions.getTransactions();
    DataSet<GraphTransaction> output = execute(input);

    return new GraphTransactions(output, gradoopFlinkConfig);
  }

  /**
   * Executes the algorithm for a dataset of graphs in transactional representation.
   *
   * @param transactions dataset of graphs
   *
   * @return frequent patterns as dataset of graphs
   */
  protected abstract DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions);

  /**
   * Triggers the label-frequency base preprocessing
   *
   * @param transactions input
   * @return preprocessed input
   */
  protected DataSet<GraphTransaction> preProcess(DataSet<GraphTransaction> transactions) {
    transactions = transactions
      .map(new DropPropertiesAndGraphContainment());

    this.graphCount = Count
      .count(transactions);

    this.minFrequency = graphCount
      .map(new MinFrequency(fsmConfig));

    DataSet<String> frequentVertexLabels = transactions
      .flatMap(new VertexLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, DIMSpanConstants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterVerticesByLabel())
      .withBroadcastSet(frequentVertexLabels, TFSMConstants.FREQUENT_VERTEX_LABELS);

    DataSet<String> frequentEdgeLabels = transactions
      .flatMap(new EdgeLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, DIMSpanConstants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterEdgesByLabel())
      .withBroadcastSet(frequentEdgeLabels, TFSMConstants.FREQUENT_EDGE_LABELS);

    transactions = transactions
      .filter(new NotEmpty());

    return transactions;
  }

  /**
   * Creates a Flink sum aggregate function that can be applied in group combine operations.
   *
   * @return sum group combine function
   */
  protected GroupCombineFunction<WithCount<TraversalCode<String>>, WithCount<TraversalCode<String>>>
  sumPartition() {

    SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };

    int[] fields = { 1 };

    return new AggregateOperator.AggregatingUdf(sum, fields);
  }
}
