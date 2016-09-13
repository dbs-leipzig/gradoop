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

package org.gradoop.flink.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.canonicalization.CAMLabeler;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;

/**
 * Superclass of classes joining subgraph embeddings.
 */
abstract class JoinEmbeddings
  implements GroupReduceFunction<SubgraphEmbeddings, SubgraphEmbeddings> {

  /**
   * Reuse tuple to avoid instantiations.
   */
  protected final SubgraphEmbeddings reuseTuple = new SubgraphEmbeddings();

  /**
   * Labeler used to generate canonical labels.
   */
  final CAMLabeler canonicalLabeler;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  JoinEmbeddings(FSMConfig fsmConfig) {
    canonicalLabeler = new CAMLabeler(fsmConfig);
  }

  /**
   * Triggers output of subgraphs and embeddings.
   *
   * @param out Flink output collector
   * @param subgraphEmbeddings subgraphs and embeddings.
   */
  protected void collect(Collector<SubgraphEmbeddings> out,
    Map<String, Collection<Embedding>> subgraphEmbeddings) {

    for (Map.Entry<String, Collection<Embedding>> entry  :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setSubgraph(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }
}
