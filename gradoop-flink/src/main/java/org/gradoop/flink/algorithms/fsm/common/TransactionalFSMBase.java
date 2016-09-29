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

package org.gradoop.flink.algorithms.fsm.common;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.config.FilterStrategy;
import org.gradoop.flink.algorithms.fsm.common.functions.CanonicalLabelOnly;
import org.gradoop.flink.algorithms.fsm.common.functions.SubgraphIsFrequent;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMGraph;
import org.gradoop.flink.algorithms.fsm.common.tuples.Subgraph;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions.GraphId;
import org.gradoop.flink.algorithms.fsm.common.functions.MergeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.functions.PatternGrowth;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;


/**
 * Superclass of transactional FSM and derivatives.
 */
public abstract class TransactionalFSMBase
  <G extends FSMGraph, S extends Subgraph, SE extends SubgraphEmbeddings>
  implements UnaryCollectionToCollectionOperator {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public TransactionalFSMBase(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  protected DataSet<SE> growEmbeddingsOfFrequentSubgraphs(
    DataSet<G> graphs, DataSet<SE> embeddings,
    DataSet<S> frequentSubgraphs) {
    embeddings = filterByFrequentSubgraphs(embeddings, frequentSubgraphs);

    embeddings = embeddings
      .groupBy(0)
      .reduceGroup(new MergeEmbeddings<SE>());

    embeddings = embeddings
      .join(graphs)
      .where(0).equalTo(new GraphId<G>())
      .with(new PatternGrowth<G, SE>(fsmConfig));
    return embeddings;
  }

  /**
   * Filter a set of embeddings to such representing a frequent subgraph.
   *
   * @param embeddings set of embeddings
   * @param frequentSubgraphs frequent subgraphs
   *
   * @return embeddings representing frequent subgraphs
   */
  protected DataSet<SE> filterByFrequentSubgraphs(
    DataSet<SE> embeddings,
    DataSet<S> frequentSubgraphs) {

    if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_FILTER) {
      return embeddings
        .filter(new SubgraphIsFrequent<SE>())
        .withBroadcastSet(
          frequentSubgraphs
            .map(new CanonicalLabelOnly<S>()),
          Constants.FREQUENT_SUBGRAPHS
        );
    } else if (fsmConfig.getFilterStrategy() == FilterStrategy.BROADCAST_JOIN) {
      return embeddings
        .joinWithTiny(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<SE, S>());
    } else {
      return embeddings
        .join(frequentSubgraphs)
        .where(2).equalTo(0) // on canonical label
        .with(new LeftSide<SE, S>());
    }
  }
}
