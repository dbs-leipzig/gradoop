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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.tuples.Subgraph;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

/**
 * (graphId, size, canonicalLabel, embeddings)
 *   => (canonicalLabel, frequency=1, sample embedding)
 */
public class SubgraphOnly implements MapFunction<SubgraphEmbeddings, Subgraph> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final Subgraph reuseTuple = new Subgraph(null, 1L, null);

  @Override
  public Subgraph map(SubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple
      .setSubgraph(subgraphEmbeddings.getSubgraph());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
