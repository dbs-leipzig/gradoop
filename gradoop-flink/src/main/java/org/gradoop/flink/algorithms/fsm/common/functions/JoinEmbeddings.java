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

package org.gradoop.flink.algorithms.fsm.common.functions;

import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;

/**
 * Superclass of classes joining subgraph embeddings.
 *
 * @param <SE> subgraph type
 */
public abstract class JoinEmbeddings<SE extends SubgraphEmbeddings> {

  /**
   * Labeler used to generate canonical labels.
   */
  protected final CanonicalLabeler canonicalLabeler;

  /**
   * FSM configuration.
   */
  protected final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  protected JoinEmbeddings(FSMConfig fsmConfig) {
    this.canonicalLabeler = fsmConfig.getCanonicalLabeler();
    this.fsmConfig = fsmConfig;
  }

  /**
   * Triggers output of subgraphs and embeddings.
   *
   * @param reuseTuple reuse tuple to avoid instantiations
   * @param out Flink output collector
   * @param subgraphEmbeddings subgraphs and embeddings.
   */
  protected void collect(SE reuseTuple, Collector<SE> out,
    Map<String, Collection<Embedding>> subgraphEmbeddings) {

    for (Map.Entry<String, Collection<Embedding>> entry  :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }
}
