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

import org.apache.flink.api.common.functions.MapFunction;

import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * FSM subgraph -> Gradoop graph transaction.
 */
public class TFSMSubgraphDecoder extends SubgraphDecoder
  implements MapFunction<TFSMSubgraph, GraphTransaction> {

  /**
   * Label of frequent subgraphs.
   */
  private static final String SUBGRAPH_LABEL = "FrequentSubgraph";

  /**
   * Constructor.
   *
   * @param config Gradoop Flink configuration
   */
  public TFSMSubgraphDecoder(GradoopFlinkConfig config) {
    super(config);
  }

  @Override
  public GraphTransaction map(TFSMSubgraph value) throws Exception {
    return createTransaction(value, SUBGRAPH_LABEL);
  }

}
