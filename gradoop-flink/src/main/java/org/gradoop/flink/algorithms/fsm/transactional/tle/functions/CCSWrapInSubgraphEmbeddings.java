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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.CCSSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class CCSWrapInSubgraphEmbeddings implements
  MapFunction<CCSSubgraph, CCSSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public CCSWrapInSubgraphEmbeddings() {
    this.reuseTuple = new CCSSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
  }

  @Override
  public CCSSubgraphEmbeddings map(CCSSubgraph subgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(subgraph.getCanonicalLabel());
    reuseTuple.setCategory(subgraph.getCategory());
    reuseTuple.setSize(subgraph.getEmbedding().getEdges().size());
    reuseTuple.setEmbeddings(Lists.newArrayList(subgraph.getEmbedding()));

    return reuseTuple;
  }
}
