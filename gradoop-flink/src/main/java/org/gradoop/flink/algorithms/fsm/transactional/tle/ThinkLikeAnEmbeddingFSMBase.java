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
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.MergeEmbeddings;
import org.gradoop.flink.algorithms.fsm.transactional.tle.functions.JoinEmbeddings;
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
   * @param parents parent embeddings
   * @param frequentSubgraphs frequent subgraphs
   * @return child embeddings
   */
  protected DataSet<SE> growEmbeddingsOfFrequentSubgraphs(
    DataSet<SE> parents,  DataSet<S> frequentSubgraphs) {

    parents = filterByFrequentSubgraphs(parents, frequentSubgraphs);

    parents = parents
      .groupBy(0)
      .reduceGroup(new MergeEmbeddings<>());

    return parents
      .flatMap(new JoinEmbeddings<>(fsmConfig));
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
