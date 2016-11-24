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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm_old.ccs.pojos.CCSGraph;
import org.gradoop.flink.algorithms.fsm_old.ccs.tuples.CCSSubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm_old.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm_old.common.functions.SingleEdgeEmbeddings;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.Embedding;

import java.util.List;
import java.util.Map;

/**
 * graph => embedding(k=1),..
 */
public class CCSSingleEdgeEmbeddings extends SingleEdgeEmbeddings
  implements FlatMapFunction<CCSGraph, CCSSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraphEmbeddings reuseTuple = new CCSSubgraphEmbeddings();

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CCSSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void flatMap(CCSGraph graph,
    Collector<CCSSubgraphEmbeddings> out) throws Exception {

    Map<String, List<Embedding>> subgraphEmbeddings =
      createEmbeddings(graph);

    reuseTuple.setCategory(graph.getCategory());
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
