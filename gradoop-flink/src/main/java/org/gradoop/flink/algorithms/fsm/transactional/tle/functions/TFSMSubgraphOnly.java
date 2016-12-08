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

package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

/**
 * (graphId, size, canonicalLabel, embeddings)
 *   => (canonicalLabel, frequency=1, sample embedding)
 */
public class TFSMSubgraphOnly
  implements MapFunction<TFSMSubgraphEmbeddings, TFSMSubgraph> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraph reuseTuple = new TFSMSubgraph(null, 1L, null);

  @Override
  public TFSMSubgraph map(
    TFSMSubgraphEmbeddings subgraphEmbeddings) throws Exception {

    reuseTuple
      .setCanonicalLabel(subgraphEmbeddings.getCanonicalLabel());

    reuseTuple
      .setEmbedding(subgraphEmbeddings.getEmbeddings().iterator().next());

    return reuseTuple;
  }
}
