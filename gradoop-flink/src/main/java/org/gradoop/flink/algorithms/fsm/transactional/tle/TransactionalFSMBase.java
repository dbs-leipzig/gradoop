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
package org.gradoop.flink.algorithms.fsm.transactional.tle;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.DropPropertiesAndGraphContainment;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.EdgeLabels;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.FilterEdgesByLabel;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.FilterVerticesByLabel;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.NotEmpty;
import org.gradoop.flink.algorithms.fsm.transactional.common.functions.VertexLabels;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MinFrequency;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.functions.tuple.ValueOfWithCount;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.operators.count.Count;
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
  protected GradoopFlinkConfig config;

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
    config = collection.getConfig();

    DataSet<GraphTransaction> input = collection
      .getGraphTransactions();

    DataSet<GraphTransaction> output = execute(input);

    return config.getGraphCollectionFactory()
      .fromTransactions(output);
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
}
