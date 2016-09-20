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

package org.gradoop.flink.algorithms.fsm.ccs;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CCSSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CCSSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CCSSubgraphOnly;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryEdgeLabels;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryFrequent;
import org.gradoop.flink.algorithms.fsm.ccs.functions
  .CategoryFrequentAndInteresting;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryGraphCounts;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryMinFrequencies;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryVertexLabels;
import org.gradoop.flink.algorithms.fsm.ccs.functions.CategoryWithCount;
import org.gradoop.flink.algorithms.fsm.ccs.functions.IsCharacteristic;
import org.gradoop.flink.algorithms.fsm.ccs.functions.LabelOnly;
import org.gradoop.flink.algorithms.fsm.ccs.functions.ToCCSGraph;
import org.gradoop.flink.algorithms.fsm.ccs.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.ccs.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.ccs.tuples.CCSSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.TransactionalFSMBase;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.functions.CanonicalLabelOnly;
import org.gradoop.flink.algorithms.fsm.common.functions
  .JoinMultiEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions
  .JoinSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions.MinEdgeCount;
import org.gradoop.flink.algorithms.fsm.common.functions.SubgraphIsFrequent;
import org.gradoop.flink.algorithms.fsm.common.functions
  .WithoutInfrequentEdgeLabels;
import org.gradoop.flink.algorithms.fsm.common.functions
  .WithoutInfrequentVertexLabels;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Map;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class CategoryCharacteristicSubgraphs extends TransactionalFSMBase {

  /**
   * Property key to store a category association.
   */
  public static final String CATEGORY_KEY = "_category";

  /**
   * minimum frequency for subgraphs to be considered to be frequent
   */
  private DataSet<Map<String, Long>> categoryCounts;

  /**
   * minimum frequency for subgraphs to be considered to be frequent
   */
  private DataSet<Map<String, Long>> categoryMinFrequencies;

  /**
   * interstingness threshold
   */
  private final float minInterestingness;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param minInterestingness minInterestingness threshold
   *
   */
  public CategoryCharacteristicSubgraphs(
    FSMConfig fsmConfig, float minInterestingness) {
    super(fsmConfig);
    this.minInterestingness = minInterestingness;
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
    DataSet<CCSGraph> fsmGraphs = transactions
      .getTransactions()
      .map(new ToCCSGraph())
      .returns(TypeExtractor.getForClass(CCSGraph.class));

    GradoopFlinkConfig gradoopConfig = transactions.getConfig();

    DataSet<GraphTransaction> dataSet = execute(fsmGraphs, gradoopConfig);

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
    DataSet<CCSGraph> graphs, GradoopFlinkConfig gradoopConfig) {

    categoryCounts = graphs
      .map(new CategoryWithCount())
      .groupBy(0)
      .sum(1)
      .reduceGroup(new CategoryGraphCounts());

    categoryMinFrequencies = categoryCounts
      .map(new CategoryMinFrequencies(fsmConfig));

    if (fsmConfig.usePreprocessing()) {
      DataSet<String> frequentVertexLabels = graphs
        .flatMap(new CategoryVertexLabels())
        .groupBy(0, 1)
        .sum(2)
        .filter(new CategoryFrequent())
        .withBroadcastSet(categoryMinFrequencies, Constants.MIN_FREQUENCY)
        .map(new LabelOnly())
        .distinct();

      graphs = graphs
        .map(new WithoutInfrequentVertexLabels<CCSGraph>())
        .withBroadcastSet(
          frequentVertexLabels, Constants.FREQUENT_VERTEX_LABELS);

      DataSet<String> frequentEdgeLabels = graphs
        .flatMap(new CategoryEdgeLabels())
        .groupBy(0, 1)
        .sum(2)
        .filter(new CategoryFrequent())
        .withBroadcastSet(categoryMinFrequencies, Constants.MIN_FREQUENCY)
        .map(new LabelOnly())
        .distinct();

      graphs = graphs
        .map(new WithoutInfrequentEdgeLabels<CCSGraph>())
        .withBroadcastSet(frequentEdgeLabels, Constants.FREQUENT_EDGE_LABELS);
    }

    DataSet<CCSSubgraphEmbeddings>
      embeddings = graphs
      .flatMap(new CCSSingleEdgeEmbeddings(fsmConfig));

    DataSet<CCSSubgraph> frequentSubgraphs =
      getCategoryFrequentSubgraphs(embeddings);

    DataSet<CCSSubgraph> characteristicSubgraphs =
      getCharacteristicSubgraphs(frequentSubgraphs);

    if (fsmConfig.getMaxEdgeCount() > 1) {
      embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

      embeddings = embeddings
        .groupBy(0)
        .reduceGroup(
          new JoinSingleEdgeEmbeddings<CCSSubgraphEmbeddings>(fsmConfig));

      frequentSubgraphs = getCategoryFrequentSubgraphs(embeddings);

      characteristicSubgraphs = characteristicSubgraphs
        .union(getCharacteristicSubgraphs(frequentSubgraphs));

      if (fsmConfig.getMaxEdgeCount() > 2) {

        for (int i = 0; i < getMaxIterations(); i++) {
          embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

          embeddings = embeddings.groupBy(0, 1).reduceGroup(
            new JoinMultiEdgeEmbeddings<CCSSubgraphEmbeddings>(fsmConfig));

          frequentSubgraphs = getCategoryFrequentSubgraphs(embeddings);
          characteristicSubgraphs = characteristicSubgraphs
            .union(getCharacteristicSubgraphs(frequentSubgraphs));
        }
      }
    }

    if (fsmConfig.getMinEdgeCount() > 1) {
      characteristicSubgraphs = characteristicSubgraphs
        .filter(new MinEdgeCount<CCSSubgraph>(fsmConfig));
    }

    return characteristicSubgraphs
      .map(new CCSSubgraphDecoder(gradoopConfig));
  }

  /**
   * Returns characteristic from category frequent subgraphs.
   *
   * @param frequentSubgraphs subgraphs, frequent in at least one category
   * @return characteristic subgraphs
   */
  private FilterOperator<CCSSubgraph> getCharacteristicSubgraphs(
    DataSet<CCSSubgraph> frequentSubgraphs) {
    return frequentSubgraphs
      .filter(new IsCharacteristic());
  }

  /**
   * Determines frequent subgraphs in a set of embeddings.
   *
   * @param embeddings set of embeddings
   * @return frequent subgraphs
   */
  private DataSet<CCSSubgraph> getCategoryFrequentSubgraphs(
    DataSet<CCSSubgraphEmbeddings> embeddings) {
    return embeddings
      .map(new CCSSubgraphOnly())
      .groupBy(0, 3)
      .sum(1)
      .groupBy(0)
      .reduceGroup(new CategoryFrequentAndInteresting(minInterestingness))
      .withBroadcastSet(categoryCounts, Constants.GRAPH_COUNT)
      .withBroadcastSet(categoryMinFrequencies, Constants.MIN_FREQUENCY);
  }

  /**
   * Filter a set of embeddings to such representing a frequent subgraph.
   *
   * @param embeddings set of embeddings
   * @param frequentSubgraphs frequent subgraphs
   *
   * @return embeddings representing frequent subgraphs
   */
  private DataSet<CCSSubgraphEmbeddings> filterByFrequentSubgraphs(
    DataSet<CCSSubgraphEmbeddings> embeddings,
    DataSet<CCSSubgraph> frequentSubgraphs) {

    frequentSubgraphs = frequentSubgraphs
      .groupBy(0)
      .reduceGroup(new First<CCSSubgraph>());

    if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_FILTER) {
      return embeddings
        .filter(new SubgraphIsFrequent<CCSSubgraphEmbeddings>())
        .withBroadcastSet(
          frequentSubgraphs
            .map(new CanonicalLabelOnly<CCSSubgraph>()),
          Constants.FREQUENT_SUBGRAPHS
        );
    } else if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_JOIN) {
      return embeddings
        .joinWithTiny(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<CCSSubgraphEmbeddings, CCSSubgraph>());
    } else {
      return embeddings
        .join(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<CCSSubgraphEmbeddings, CCSSubgraph>());
    }
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
