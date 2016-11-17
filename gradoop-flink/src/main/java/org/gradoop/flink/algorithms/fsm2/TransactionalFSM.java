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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.SumAggregationFunction;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.common.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm2.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm2.functions.FilterEdgesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.FilterVerticesByLabel;
import org.gradoop.flink.algorithms.fsm2.functions.SortedDictionary;
import org.gradoop.flink.algorithms.fsm2.functions.ToAdjacencyList;
import org.gradoop.flink.algorithms.fsm2.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.representation.tuples.AdjacencyList;
import org.gradoop.flink.representation.tuples.GraphTransaction;
import org.gradoop.flink.util.Collect;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

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

    DataSet<GraphTransaction> transactions = input.getTransactions();

    this.minFrequency = Count.count(transactions);

    DataSet<Collection<String>> frequentVertexLabels = transactions
      .flatMap(new VertexLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>())
      .reduceGroup(new Collect<>());

    transactions = transactions
      .map(new FilterVerticesByLabel())
      .withBroadcastSet(frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

    DataSet<Collection<String>> frequentEdgeLabels = transactions
      .flatMap(new EdgeLabels())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<>())
      .reduceGroup(new Collect<>());

    transactions = transactions
      .map(new FilterEdgesByLabel())
      .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);

    DataSet<AdjacencyList<LabelPair>> adjacencyLists = transactions
      .map(new ToAdjacencyList());




//    DataSet<Map<String, Integer>> edgeDictionary;
//
//    DataSet<WithCount<String>> vertexLabelFrequencies = statistics
//
//
//    if (fsmConfig.isPreprocessingEnabled()) {
//      vertexDictionary = vertexLabelFrequencies
//
//
//      edgeDictionary = statistics
//        .flatMap(new FilteredEdgeLabels())
//        .withBroadcastSet(
//          vertexDictionary, Constants.VERTEX_DICTIONARY)
//        .groupBy(0)
//        .sum(1)
//        .filter(new Frequent<>())
//        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
//        .reduceGroup(new SortedDictionary());
//
//    } else {
//      vertexDictionary = statistics
//        .flatMap(new VertexLabels())
//        .map(new ValueOfWithCount<>())
//        .distinct()
//        .reduceGroup(new RandomDictionary());
//
//      edgeDictionary = statistics
//        .flatMap(new EdgeLabels())
//        .distinct()
//        .reduceGroup(new RandomDictionary());
//    }
//
//    DataSet<GraphEmbeddings> graphEmbeddings = graphs
//      .map(new ToGraphEmbeddings(fsmConfig))
//      .withBroadcastSet(vertexDictionary, Constants.VERTEX_DICTIONARY)
//      .withBroadcastSet(edgeDictionary, Constants.EDGE_DICTIONARY);
//
//    DataSet<WithCount<CompressedDfsCode>> allFrequentSubgraphs;
//
//    if (fsmConfig.getIterationStrategy() == IterationStrategy.LOOP_UNROLLING) {
//      allFrequentSubgraphs = mineWithLoopUnrolling(graphEmbeddings);
//    } else  {
//      allFrequentSubgraphs = mineWithBulkIteration(
//        graphEmbeddings, transactions.getConfig());
//    }
//
//    if (fsmConfig.getMinEdgeCount() > 1) {
//      allFrequentSubgraphs = allFrequentSubgraphs
//        .filter(new MinEdgeCount(fsmConfig));
//    }
//
//    return allFrequentSubgraphs
//      .map(new DFSDecoder(transactions.getConfig()))
//      .withBroadcastSet(vertexDictionary, Constants.VERTEX_DICTIONARY)
//      .withBroadcastSet(edgeDictionary, Constants.EDGE_DICTIONARY);

    return null;
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

//  /**
//   * Mining core based on loop unrolling.
//   *
//   * @param graphEmbeddings
//   * @return frequent subgraphs
//   */
//  private DataSet<WithCount<CompressedDfsCode>> mineWithLoopUnrolling(
//    DataSet<GraphEmbeddings> graphEmbeddings) {
//
//    DataSet<WithCount<CompressedDfsCode>> frequentSubgraphs =
//      getFrequentSubgraphs(graphEmbeddings);
//
//    DataSet<WithCount<CompressedDfsCode>> allFrequentSubgraphs = frequentSubgraphs;
//
//    if (fsmConfig.getMaxEdgeCount() > 1) {
//      for (int k = 1; k < fsmConfig.getMaxEdgeCount(); k++) {
//
//        graphEmbeddings =
//          growEmbeddingsOfFrequentSubgraphs(graphEmbeddings, frequentSubgraphs);
//
//        frequentSubgraphs = getFrequentSubgraphs(graphEmbeddings);
//        allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);
//      }
//    }
//
//    return allFrequentSubgraphs;
//  }
//
//  /**
//   * Mining core based on bulk iteration.
//   *
//   * @param graphEmbeddings
//   * @param config
//   * @return frequent subgraphs
//   */
//  private DataSet<WithCount<CompressedDfsCode>> mineWithBulkIteration(
//    DataSet<GraphEmbeddings> graphEmbeddings, GradoopFlinkConfig config) {
//
//    DataSet<GraphEmbeddings> collector = config
//      .getExecutionEnvironment()
//      .fromElements(0)
//      .map(new Collector());
//
//    // ITERATION HEAD
//
//    IterativeDataSet<GraphEmbeddings> iterative = graphEmbeddings
//      .union(collector)
//      .iterate(fsmConfig.getMaxEdgeCount());
//
//    // ITERATION BODY
//
//    DataSet<WithCount<CompressedDfsCode>> frequentSubgraphs =
//      getFrequentSubgraphs(iterative);
//
//    DataSet<GraphEmbeddings> nextEmbeddings =
//      growEmbeddingsOfFrequentSubgraphs(iterative, frequentSubgraphs);
//
//    // ITERATION FOOTER
//
//    return iterative
//      .closeWith(nextEmbeddings, frequentSubgraphs)
//      .filter(new IsResult(true))
//      .flatMap(new ReportResultDFSCodes());
//  }
//
//  /**
//   * Determines frequent subgraphs in a set of embeddings.
//   *
//   * @param embeddings set of embeddings
//   * @return frequent subgraphs
//   */
//  private DataSet<WithCount<CompressedDfsCode>> getFrequentSubgraphs(
//    DataSet<GraphEmbeddings> embeddings) {
//
//    DataSet<WithCount<CompressedDfsCode>> reports =
//      embeddings.flatMap(new ReportDFSCodes(fsmConfig));
//
//    if (fsmConfig.getValidationTime().equals(ValidationTime.GRAPH)) {
//      reports = reports
//        .filter(new ValidDFSCode(fsmConfig));
//
//    } else if (fsmConfig.getValidationTime().equals(ValidationTime.WORKER)) {
//      SumAggregationFunction.LongSumAgg[] sum = { new SumAggregationFunction.LongSumAgg() };
//
//      int[] fields = { 1 };
//
//      AggregateOperator.AggregatingUdf udf =
//      new AggregateOperator.AggregatingUdf(sum, fields);
//
//      reports = reports
//        .groupBy(0)
//        .combineGroup(udf)
//        .filter(new ValidDFSCode(fsmConfig));
//    }
//
//    DataSet<WithCount<CompressedDfsCode>> frequentSubgraphs = reports
//      .groupBy(0)
//      .sum(1)
//      .filter(new Frequent<>())
//      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY);
//
//    if (fsmConfig.getValidationTime().equals(ValidationTime.GLOBAL)) {
//      frequentSubgraphs = frequentSubgraphs
//        .filter(new ValidDFSCode(fsmConfig));
//    }
//
//    return frequentSubgraphs;
//  }
//
//  /**
//   * Grows children of embeddings of frequent subgraphs.
//   * @param graphEmbeddings
//   * @param frequentSubgraphs frequent subgraphs  @return child embeddings
//   */
//  private DataSet<GraphEmbeddings> growEmbeddingsOfFrequentSubgraphs(
//    DataSet<GraphEmbeddings> graphEmbeddings,
//    DataSet<WithCount<CompressedDfsCode>> frequentSubgraphs) {
//
//    return graphEmbeddings
//      .map(new PatternGrowth(fsmConfig))
//      .withBroadcastSet(
//        frequentSubgraphs.map(new ValueOfWithCount<>()),
//        Constants.FREQUENT_SUBGRAPHS)
//      .filter(new CouldGrow());
//  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
