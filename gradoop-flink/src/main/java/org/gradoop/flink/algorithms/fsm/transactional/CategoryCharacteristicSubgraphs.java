/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.transactional;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CCSSingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CCSSubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CCSSubgraphOnly;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CCSWrapInSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryEdgeLabels;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryFrequent;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryFrequentAndInteresting;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryGraphCounts;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryMinFrequencies;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryVertexLabels;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.CategoryWithCount;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.IsCharacteristic;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.LabelOnly;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.ToCCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.ThinkLikeAnEmbeddingFSMBase;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MinEdgeCount;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.WithoutInfrequentEdgeLabels;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.WithoutInfrequentVertexLabels;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.IsResult;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Map;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class CategoryCharacteristicSubgraphs
  extends ThinkLikeAnEmbeddingFSMBase<CCSGraph, CCSSubgraph, CCSSubgraphEmbeddings> {

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

  /**
   * Executes the algorithm based on transactional representation.
   *
   * @param transactions search space
   * @return category characteristic subgraphs
   */
  public DataSet<GraphTransaction> execute(DataSet<GraphTransaction> transactions) {

    DataSet<CCSGraph>  graphs = transactions
      .map(new ToCCSGraph());

    setCategoryCounts(graphs);

    setMinFrequencies();

    if (fsmConfig.isPreprocessingEnabled()) {
      graphs = preProcessCategories(graphs);
    }

    DataSet<CCSSubgraphEmbeddings> embeddings = graphs
      .flatMap(new CCSSingleEdgeEmbeddings(fsmConfig));

    // ITERATION HEAD
    IterativeDataSet<CCSSubgraphEmbeddings> iterative = embeddings
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    // get frequent subgraphs
    DataSet<CCSSubgraphEmbeddings> parentEmbeddings = iterative
      .filter(new IsResult<>(false));

    DataSet<CCSSubgraph> categoryFrequentSubgraphs =
      getCategoryFrequentSubgraphs(parentEmbeddings);

    DataSet<CCSSubgraph> frequentSubgraphs = categoryFrequentSubgraphs
      .groupBy(0)
      .reduceGroup(new First<>());

    parentEmbeddings =
      filterByFrequentSubgraphs(parentEmbeddings, frequentSubgraphs);

    DataSet<CCSSubgraphEmbeddings> childEmbeddings =
      growEmbeddingsOfFrequentSubgraphs(parentEmbeddings, frequentSubgraphs
      );

    DataSet<CCSSubgraphEmbeddings> resultIncrement =
      getCharacteristicSubgraphs(categoryFrequentSubgraphs)
        .map(new CCSWrapInSubgraphEmbeddings());

    DataSet<CCSSubgraphEmbeddings> resultAndEmbeddings = iterative
      .filter(new IsResult<>(true))
      .union(resultIncrement)
      .union(childEmbeddings);

    // ITERATION FOOTER

    DataSet<CCSSubgraph> characteristicSubgraphs = iterative
      .closeWith(resultAndEmbeddings, childEmbeddings)
      .filter(new IsResult<>(true))
      .map(new CCSSubgraphOnly());

    if (fsmConfig.getMinEdgeCount() > 1) {
      characteristicSubgraphs = characteristicSubgraphs
        .filter(new MinEdgeCount<>(fsmConfig));
    }

    return characteristicSubgraphs
      .map(new CCSSubgraphDecoder(config));
  }

  /**
   * Determines the graph count for each category.
   *
   * @param graphs search space
   */
  private void setCategoryCounts(DataSet<CCSGraph> graphs) {
    categoryCounts = graphs
      .map(new CategoryWithCount())
      .groupBy(0)
      .sum(1)
      .reduceGroup(new CategoryGraphCounts());
  }

  /**
   * Determines the min frequency per category based on category graph count
   * and support threshold.
   **/
  private void setMinFrequencies() {
    categoryMinFrequencies = categoryCounts
      .map(new CategoryMinFrequencies(fsmConfig));
  }

  /**
   * Removes vertices and edges showing labels that are infrequent in all
   * categories.
   *
   * @param graphs graph collection
   * @return processed graph collection
   */
  private DataSet<CCSGraph> preProcessCategories(DataSet<CCSGraph> graphs) {
    DataSet<String> frequentVertexLabels = graphs
      .flatMap(new CategoryVertexLabels())
      .groupBy(0, 1)
      .sum(2)
      .filter(new CategoryFrequent())
      .withBroadcastSet(categoryMinFrequencies, DIMSpanConstants.MIN_FREQUENCY)
      .map(new LabelOnly())
      .distinct();

    graphs = graphs
      .map(new WithoutInfrequentVertexLabels<CCSGraph>())
      .withBroadcastSet(
        frequentVertexLabels, TFSMConstants.FREQUENT_VERTEX_LABELS);

    DataSet<String> frequentEdgeLabels = graphs
      .flatMap(new CategoryEdgeLabels())
      .groupBy(0, 1)
      .sum(2)
      .filter(new CategoryFrequent())
      .withBroadcastSet(categoryMinFrequencies, DIMSpanConstants.MIN_FREQUENCY)
      .map(new LabelOnly())
      .distinct();

    graphs = graphs
      .map(new WithoutInfrequentEdgeLabels<CCSGraph>())
      .withBroadcastSet(frequentEdgeLabels, TFSMConstants.FREQUENT_EDGE_LABELS);

    return graphs;
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
      .withBroadcastSet(categoryCounts, TFSMConstants.GRAPH_COUNT)
      .withBroadcastSet(categoryMinFrequencies, DIMSpanConstants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

}
