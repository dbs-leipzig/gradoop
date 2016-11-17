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

package org.gradoop.flink.algorithms.fsm.tfsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.flink.algorithms.fsm.common.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.IterationStrategy;
import org.gradoop.flink.algorithms.fsm.common.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.common.functions.MinEdgeCount;
import org.gradoop.flink.algorithms.fsm.common.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm.common.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm.common.functions.WithoutInfrequentEdgeLabels;
import org.gradoop.flink.algorithms.fsm.common.functions.WithoutInfrequentVertexLabels;
import org.gradoop.flink.algorithms.fsm.common.functions.IsResult;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSubgraphOnly;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.ToTFSMGraph;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMWrapInSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.representation.transaction.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM
  extends TransactionalFSMBase<TFSMGraph, TFSMSubgraph, TFSMSubgraphEmbeddings>
{

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
    super(fsmConfig);
  }

  @Override
  public GraphCollection execute(GraphCollection collection)  {
    GradoopFlinkConfig gradoopFlinkConfig = collection.getConfig();

    // create transactions from graph collection
    GraphTransactions transactions = collection
      .toTransactions();

    transactions = execute(transactions);

    return GraphCollection.fromTransactions(
      new GraphTransactions(
        transactions.getTransactions(), gradoopFlinkConfig));
  }

  /**
   * Encodes the search space before executing FSM.
   *
   * @param transactions search space as Gradoop graph transactions
   *
   * @return frequent subgraphs as Gradoop graph transactions
   */
  public GraphTransactions execute(GraphTransactions transactions) {
    DataSet<TFSMGraph> graphs = transactions
      .getTransactions()
      .map(new ToTFSMGraph())
      .returns(TypeExtractor.getForClass(TFSMGraph.class));

    GradoopFlinkConfig gradoopConfig = transactions.getConfig();

    DataSet<GraphTransaction> dataSet = execute(graphs, gradoopConfig);

    return new GraphTransactions(dataSet, gradoopConfig);
  }

  /**
   * Core mining method.
   *
   * @param graphs search space
   * @param gradoopConfig Gradoop Flink configuration
   *
   * @return frequent subgraphs
   */
  public DataSet<GraphTransaction> execute(
    DataSet<TFSMGraph> graphs, GradoopFlinkConfig gradoopConfig) {

    setMinFrequency(graphs);

    if (fsmConfig.isPreprocessingEnabled()) {
      graphs = preProcess(graphs);
    }

    DataSet<TFSMSubgraph> allFrequentSubgraphs;

    DataSet<TFSMSubgraphEmbeddings> embeddings = graphs
      .flatMap(new TFSMSingleEdgeEmbeddings(fsmConfig));

    if (fsmConfig.getIterationStrategy() == IterationStrategy.LOOP_UNROLLING) {
      allFrequentSubgraphs = mineWithLoopUnrolling(graphs, embeddings);
    } else  {
      allFrequentSubgraphs = mineWithBulkIteration(graphs, embeddings);
    }

    if (fsmConfig.getMinEdgeCount() > 1) {
      allFrequentSubgraphs = allFrequentSubgraphs
        .filter(new MinEdgeCount<TFSMSubgraph>(fsmConfig));
    }

    return allFrequentSubgraphs
      .map(new TFSMSubgraphDecoder(gradoopConfig));
  }

  /**
   * Determines the min frequency based on graph count and support threshold.
   *
   * @param graphs search space
   */
  private void setMinFrequency(DataSet<TFSMGraph> graphs) {
    minFrequency = Count
      .count(graphs)
      .map(new MinFrequency(fsmConfig));
  }

  /**
   * Removes vertices and edges showing infrequent labels.
   *
   * @param graphs search space
   * @return processed search space
   */
  private DataSet<TFSMGraph> preProcess(DataSet<TFSMGraph> graphs) {
    DataSet<String> frequentVertexLabels = graphs
      .flatMap(new VertexLabels<TFSMGraph>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<WithCount<String>>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<String>());

    graphs = graphs
      .map(new WithoutInfrequentVertexLabels<TFSMGraph>())
      .withBroadcastSet(
        frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

    DataSet<String> frequentEdgeLabels = graphs
      .flatMap(new EdgeLabels<TFSMGraph>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<WithCount<String>>())
      .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
      .map(new ValueOfWithCount<String>());

    graphs = graphs
      .map(new WithoutInfrequentEdgeLabels<TFSMGraph>())
      .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);
    return graphs;
  }

  /**
   * Mining core based on loop unrolling.
   *
   * @param graphs search space
   * @param embeddings 1-edge embeddings
   * @return frequent subgraphs
   */
  private DataSet<TFSMSubgraph> mineWithLoopUnrolling(
    DataSet<TFSMGraph> graphs, DataSet<TFSMSubgraphEmbeddings> embeddings) {

    DataSet<TFSMSubgraph> frequentSubgraphs = getFrequentSubgraphs(embeddings);
    DataSet<TFSMSubgraph> allFrequentSubgraphs = frequentSubgraphs;

    if (fsmConfig.getMaxEdgeCount() > 1) {
      for (int k = fsmConfig.getMinEdgeCount();
           k < fsmConfig.getMaxEdgeCount(); k++) {

        embeddings = growEmbeddingsOfFrequentSubgraphs(
          graphs, embeddings, frequentSubgraphs);

        frequentSubgraphs = getFrequentSubgraphs(embeddings);
        allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);
      }
    }

    return allFrequentSubgraphs;
  }

  /**
   * Mining core based on bulk iteration.
   *
   * @param graphs search space
   * @param embeddings 1-edge embeddings
   * @return frequent subgraphs
   */
  private DataSet<TFSMSubgraph> mineWithBulkIteration(
    DataSet<TFSMGraph> graphs, DataSet<TFSMSubgraphEmbeddings> embeddings) {

    // ITERATION HEAD
    IterativeDataSet<TFSMSubgraphEmbeddings> iterative = embeddings
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    // get frequent subgraphs
    DataSet<TFSMSubgraphEmbeddings> parentEmbeddings = iterative
      .filter(new IsResult<TFSMSubgraphEmbeddings>(false));

    DataSet<TFSMSubgraph> frequentSubgraphs =
      getFrequentSubgraphs(parentEmbeddings);

    parentEmbeddings =
      filterByFrequentSubgraphs(parentEmbeddings, frequentSubgraphs);

    DataSet<TFSMSubgraphEmbeddings> childEmbeddings =
      growEmbeddingsOfFrequentSubgraphs(
        graphs, parentEmbeddings, frequentSubgraphs);

    DataSet<TFSMSubgraphEmbeddings> resultIncrement = frequentSubgraphs
      .map(new TFSMWrapInSubgraphEmbeddings());

    DataSet<TFSMSubgraphEmbeddings> resultAndEmbeddings = iterative
      .filter(new IsResult<TFSMSubgraphEmbeddings>(true))
      .union(resultIncrement)
      .union(childEmbeddings);

    // ITERATION FOOTER

    return iterative
      .closeWith(resultAndEmbeddings, childEmbeddings)
      .filter(new IsResult<TFSMSubgraphEmbeddings>(true))
      .map(new TFSMSubgraphOnly());
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
        .filter(new Frequent<TFSMSubgraph>())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
