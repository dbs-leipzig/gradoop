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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.TFSMSubgraphEmbeddings;

import java.util.List;
import java.util.Map;

/**
 * graph => embedding(k=1),..
 */
public class TFSMSingleEdgeEmbeddings extends SingleEdgeEmbeddings
  implements FlatMapFunction<TFSMGraph, TFSMSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraphEmbeddings reuseTuple =
    new TFSMSubgraphEmbeddings();

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public TFSMSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void flatMap(
    TFSMGraph graph, Collector<TFSMSubgraphEmbeddings> out) throws Exception {

    Map<String, List<Embedding>> subgraphEmbeddings =
      createEmbeddings(graph);

    reuseTuple.setGraphId(graph.getId());
    reuseTuple.setSize(1);

    for (Map.Entry<String, List<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }

}
