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

package org.gradoop.flink.algorithms.fsm.tfsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.functions.SingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.tfsm.pojos.TFSMGraph;
import org.gradoop.flink.algorithms.fsm.tfsm.tuples.TFSMSubgraphEmbeddings;

import java.util.Collection;
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

    Map<String, Collection<Embedding>> subgraphEmbeddings =
      createEmbeddings(graph);

    reuseTuple.setGraphId(graph.getId());
    reuseTuple.setSize(1);

    for (Map.Entry<String, Collection<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }

}
