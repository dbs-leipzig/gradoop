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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.flink.algorithms.fsm.common.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.functions.CanonicalLabelOnly;
import org.gradoop.flink.algorithms.fsm.common.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm.common.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.common.functions
  .JoinMultiEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions
  .JoinSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions.MinEdgeCount;
import org.gradoop.flink.algorithms.fsm.common.functions.MinFrequency;
import org.gradoop.flink.algorithms.fsm.common.functions.SubgraphIsFrequent;
import org.gradoop.flink.algorithms.fsm.common.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm.common.functions
  .WithoutInfrequentEdgeLabels;
import org.gradoop.flink.algorithms.fsm.common.functions
  .WithoutInfrequentVertexLabels;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.TFSMSubgraphOnly;
import org.gradoop.flink.algorithms.fsm.tfsm.functions.ToTFSMGraph;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM extends TransactionalFSMBase {

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
    DataSet<TFSMGraph> fsmGraphs = transactions
      .getTransactions()
      .map(new ToTFSMGraph())
      .returns(TypeExtractor.getForClass(TFSMGraph.class));

    GradoopFlinkConfig gradoopConfig = transactions.getConfig();

    DataSet<GraphTransaction> dataSet = execute(fsmGraphs, gradoopConfig);

    return new GraphTransactions(dataSet, gradoopConfig);
  }

  /**
   * Core mining method.
   *
   * @param fsmGraphs search space
   * @param gradoopConfig Gradoop Flink configuration
   *
   * @return frequent subgraphs
   */
  public DataSet<GraphTransaction> execute(
    DataSet<TFSMGraph> fsmGraphs, GradoopFlinkConfig gradoopConfig) {

    minFrequency = Count
      .count(fsmGraphs)
      .map(new MinFrequency(fsmConfig));

    if (fsmConfig.usePreprocessing()) {
      DataSet<String> frequentVertexLabels = fsmGraphs
        .flatMap(new VertexLabels<TFSMGraph>())
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<WithCount<String>>())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
        .map(new ValueOfWithCount<String>());

      fsmGraphs = fsmGraphs
        .map(new WithoutInfrequentVertexLabels<TFSMGraph>())
        .withBroadcastSet(
          frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

      DataSet<String> frequentEdgeLabels = fsmGraphs
        .flatMap(new EdgeLabels<TFSMGraph>())
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<WithCount<String>>())
        .withBroadcastSet(minFrequency, Constants.MIN_FREQUENCY)
        .map(new ValueOfWithCount<String>());

      fsmGraphs = fsmGraphs
        .map(new WithoutInfrequentEdgeLabels<TFSMGraph>())
        .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);
    }

    DataSet<TFSMSubgraphEmbeddings> embeddings = fsmGraphs
      .flatMap(new TFSMSingleEdgeEmbeddings(fsmConfig));

    DataSet<TFSMSubgraph> frequentSubgraphs =
      getFrequentSubgraphs(embeddings);

    DataSet<TFSMSubgraph> allFrequentSubgraphs =
      frequentSubgraphs;

    if (fsmConfig.getMaxEdgeCount() > 1) {
      embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

      embeddings = embeddings
        .groupBy(0)
        .reduceGroup(
          new JoinSingleEdgeEmbeddings<TFSMSubgraphEmbeddings>(fsmConfig));

      frequentSubgraphs = getFrequentSubgraphs(embeddings);
      allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);

      if (fsmConfig.getMaxEdgeCount() > 2) {

        for (int i = 0; i <= getMaxIterations(); i++) {
          embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

          embeddings = embeddings.groupBy(0, 1).reduceGroup(
            new JoinMultiEdgeEmbeddings<TFSMSubgraphEmbeddings>(fsmConfig));

          frequentSubgraphs = getFrequentSubgraphs(embeddings);
          allFrequentSubgraphs = allFrequentSubgraphs.union(frequentSubgraphs);
        }
      }
    }

    if (fsmConfig.getMinEdgeCount() > 1) {
      allFrequentSubgraphs = allFrequentSubgraphs
        .filter(new MinEdgeCount<TFSMSubgraph>(fsmConfig));
    }

    return allFrequentSubgraphs
      .map(new TFSMSubgraphDecoder(gradoopConfig));
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

  /**
   * Filter a set of embeddings to such representing a frequent subgraph.
   *
   * @param embeddings set of embeddings
   * @param frequentSubgraphs frequent subgraphs
   *
   * @return embeddings representing frequent subgraphs
   */
  private DataSet<TFSMSubgraphEmbeddings> filterByFrequentSubgraphs(
    DataSet<TFSMSubgraphEmbeddings> embeddings,
    DataSet<TFSMSubgraph> frequentSubgraphs) {

    if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_FILTER) {
      return embeddings
        .filter(new SubgraphIsFrequent<TFSMSubgraphEmbeddings>())
        .withBroadcastSet(
          frequentSubgraphs
            .map(new CanonicalLabelOnly<TFSMSubgraph>()),
          Constants.FREQUENT_SUBGRAPHS
        );
    } else if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_JOIN) {
      return embeddings
        .joinWithTiny(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<TFSMSubgraphEmbeddings, TFSMSubgraph>());
    } else {
      return embeddings
        .join(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<TFSMSubgraphEmbeddings, TFSMSubgraph>());
    }
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
