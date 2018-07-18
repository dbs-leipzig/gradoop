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
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class TFSMWrapInSubgraphEmbeddings implements
  MapFunction<TFSMSubgraph, TFSMSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public TFSMWrapInSubgraphEmbeddings() {
    this.reuseTuple = new TFSMSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
    this.reuseTuple.setSize(0);
  }

  @Override
  public TFSMSubgraphEmbeddings map(TFSMSubgraph tfsmSubgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(tfsmSubgraph.getCanonicalLabel());
    reuseTuple.setEmbeddings(Lists.newArrayList(tfsmSubgraph.getEmbedding()));

    return reuseTuple;
  }
}
