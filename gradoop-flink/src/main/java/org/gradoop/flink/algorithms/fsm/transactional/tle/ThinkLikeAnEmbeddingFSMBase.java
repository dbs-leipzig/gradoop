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

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.GraphId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MergeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.PatternGrowth;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.Subgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;


/**
 * Superclass of transactional FSM and derivatives.
 *
 * @param <G> graph type
 * @param <S> subgraph type
 * @param <SE> subgraph embeddings type
 */
public abstract class ThinkLikeAnEmbeddingFSMBase
  <G extends FSMGraph, S extends Subgraph, SE extends SubgraphEmbeddings>
  extends TransactionalFSMBase {

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public ThinkLikeAnEmbeddingFSMBase(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  /**
   * Grows children of embeddings of frequent subgraphs.
   *
   * @param graphs search space
   * @param parents parent embeddings
   * @param frequentSubgraphs frequent subgraphs
   * @return child embeddings
   */
  protected DataSet<SE> growEmbeddingsOfFrequentSubgraphs(
    DataSet<G> graphs, DataSet<SE> parents,
    DataSet<S> frequentSubgraphs) {
    parents = filterByFrequentSubgraphs(parents, frequentSubgraphs);

    parents = parents
      .groupBy(0)
      .reduceGroup(new MergeEmbeddings<>());

    parents = parents
      .join(graphs)
      .where(0).equalTo(new GraphId<>())
      .with(new PatternGrowth<>(fsmConfig));

    return parents;
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

    return embeddings
      .joinWithTiny(frequentSubgraphs)
      .where(2).equalTo(0) // on canonical label
      .with(new LeftSide<>());
  }
}
