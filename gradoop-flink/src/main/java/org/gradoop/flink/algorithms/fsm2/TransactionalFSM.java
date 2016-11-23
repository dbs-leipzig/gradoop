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

package org.gradoop.flink.algorithms.fsm2;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.common.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm2.functions.DropPropertiesAndGraphContainment;
import org.gradoop.flink.algorithms.fsm2.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm2.functions.FilterEdgesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.FilterVerticesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.Report;
import org.gradoop.flink.algorithms.fsm2.functions.ToAdjacencyList;
import org.gradoop.flink.algorithms.fsm2.functions.InitSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm2.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm2.gspan.EmptyGraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm2.gspan.ExpandResult;
import org.gradoop.flink.algorithms.fsm2.gspan.HasEmbeddings;
import org.gradoop.flink.algorithms.fsm2.gspan.PatternGrowth;
import org.gradoop.flink.algorithms.fsm2.gspan.ToGraphTransaction;
import org.gradoop.flink.algorithms.fsm2.gspan.Validate;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator
{

  private final FSMConfig fsmConfig;
  /**
   * minimum frequency for subgraphs to be considered to be frequent
   */
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
    GraphTransactions transactions = collection
      .toTransactions();

    return GraphCollection.fromTransactions(
      new GraphTransactions(execute(transactions), gradoopFlinkConfig));
  }

  /**
   * Encodes the search space before executing FSM.
   *
   * @param input search space as Gradoop graph transactions
   *
   * @return frequent subgraphs as Gradoop graph transactions
   */
  public DataSet<GraphTransaction> execute(GraphTransactions input) {

    DataSet<GraphTransaction> transactions = input
      .getTransactions()
      .map(new DropPropertiesAndGraphContainment());

    this.minFrequency = Count.count(transactions);

    DataSet<String> frequentVertexLabels = transactions
      .flatMap(new VertexLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterVerticesByLabel())
      .withBroadcastSet(frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

    DataSet<String> frequentEdgeLabels = transactions
      .flatMap(new EdgeLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    transactions = transactions
      .map(new FilterEdgesByLabel())
      .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);

    DataSet<GraphEmbeddingPair> searchSpace = transactions
      .map(new ToAdjacencyList())
      .map(new InitSingleEdgeEmbeddings());

    DataSet<GraphEmbeddingPair> collector = input
      .getConfig()
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new EmptyGraphEmbeddingPair());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<GraphEmbeddingPair> iterative = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    DataSet<TraversalCode<String>> frequentPatterns = iterative
      .flatMap(new Report())
      .groupBy(0)
      .combineGroup(sumPartition())
      .filter(new Validate())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>());

    DataSet<GraphEmbeddingPair> grownEmbeddings = iterative
      .map(new PatternGrowth())
      .withBroadcastSet(frequentPatterns, Constants.FREQUENT_SUBGRAPHS)
      .filter(new HasEmbeddings());

    // ITERATION FOOTER

    return iterative
      .closeWith(grownEmbeddings, frequentPatterns)
      .flatMap(new ExpandResult())
      .map(new ToGraphTransaction());
  }

  private GroupCombineFunction
    <WithCount<TraversalCode<String>>, WithCount<TraversalCode<String>>> sumPartition() {

    SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };

    int[] fields = { 1 };

    return new AggregateOperator.AggregatingUdf(sum, fields);
  }

  /**
   * Determines the min frequency based on graph count and support threshold.
   *
   * @param graphs search space
   */
  private void setMinFrequency(DataSet<Map<String, Collection<String>>> graphs) {
    minFrequency = Count
      .count(graphs)
      .map(new MinFrequency(fsmConfig));
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
