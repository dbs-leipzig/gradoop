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

package org.gradoop.flink.algorithms.fsm_old.ccs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm_old.ccs.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm_old.ccs.tuples.CCSSubgraphEmbeddings;

/**
 * (graphId, size, canonicalLabel, embeddings)
 *   => (canonicalLabel, frequency=1, sample embedding)
 */
public class CCSSubgraphOnly
  implements MapFunction<CCSSubgraphEmbeddings, CCSSubgraph> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraph reuseTuple =
    new CCSSubgraph(null, null, 1L, null, false);

  @Override
  public CCSSubgraph map(
    CCSSubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple.setCategory(subgraphEmbeddings.getCategory());

    reuseTuple
      .setCanonicalLabel(subgraphEmbeddings.getCanonicalLabel());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
