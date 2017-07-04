/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;

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
