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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;

import java.util.Collection;

/**
 * (workerId, {subgraph,..}) |><| (workerId, {graph,..}) =>
 *   (subgraph, frequency),..
 */
public class Refinement implements FlatJoinFunction<
  Tuple2<Integer, Collection<CompressedDFSCode>>,
  Tuple2<Integer, Collection<GSpanGraph>>, WithCount<CompressedDFSCode>> {

  /**
   * FSM configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public Refinement(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void join(
    Tuple2<Integer, Collection<CompressedDFSCode>> partitionSubgraphs,
    Tuple2<Integer, Collection<GSpanGraph>> partitionGraphs,
    Collector<WithCount<CompressedDFSCode>> collector) throws Exception {

    Collection<CompressedDFSCode> refinementSubgraphs = partitionSubgraphs.f1;
    Collection<GSpanGraph> graphs = partitionGraphs.f1;

    for (CompressedDFSCode compressedSubgraph : refinementSubgraphs) {

      DFSCode subgraph = compressedSubgraph.getDfsCode();
      int frequency = 0;

      for (GSpanGraph graph : graphs) {
        if (GSpan.contains(graph, subgraph, fsmConfig)) {
          frequency++;
        }
      }

      if (frequency > 0) {
        WithCount<CompressedDFSCode> subgraphWithCount =
          new WithCount<>(compressedSubgraph, frequency);

        collector.collect(subgraphWithCount);
      }
    }
  }
}
