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

import java.util.Collection;
import java.util.Map;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.FilterMessage;

/**
 * GSpan algorithm controller for a partition of graph graphs.
 */
public class PartitionGSpan implements FlatMapFunction
  <Tuple2<Integer, Collection<GSpanGraph>>, FilterMessage> {

  /**
   * FSM configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   */
  public PartitionGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(Tuple2<Integer, Collection<GSpanGraph>> pair,
    Collector<FilterMessage> collector) throws Exception {

    Collection<GSpanGraph> graphs = pair.f1;

    int graphCount = graphs.size();
    int minSupport = (int) (fsmConfig.getMinSupport() * (float) graphCount) - 1;
    int minLikelySupport =
      (int) (fsmConfig.getLikelinessThreshold() * (float) graphCount) - 1;

    Collection<WithCount<DFSCode>> allLocallyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DFSCode>> likelyFrequentSubgraphs =
      Lists.newArrayList();
    Collection<WithCount<DFSCode>> currentFrequentSubgraphs;

    int edgeCount = 1;
    do {
      // count frequency
      Map<DFSCode, Integer> subgraphSupport = countFrequency(graphs);

      currentFrequentSubgraphs = Lists.newArrayList();

      for (Map.Entry<DFSCode, Integer> entry : subgraphSupport.entrySet()) {

        DFSCode subgraph = entry.getKey();
        int frequency = entry.getValue();

        if (frequency >= minSupport) {
          if (GSpan.isMinimal(subgraph, fsmConfig)) {
            WithCount<DFSCode> subgraphWithCount =
              new WithCount<>(subgraph, frequency);
            currentFrequentSubgraphs.add(subgraphWithCount);
            allLocallyFrequentSubgraphs.add(subgraphWithCount);
          }
        } else if (frequency >= minLikelySupport) {
          if (GSpan.isMinimal(subgraph, fsmConfig)) {
            likelyFrequentSubgraphs.add(new WithCount<>(subgraph, frequency));
          }
        }
      }

      for (GSpanGraph graph : graphs) {
        if (graph.hasGrownSubgraphs()) {
          GSpan.growEmbeddings(
            graph, removeCounts(currentFrequentSubgraphs), fsmConfig);
        }
      }

      edgeCount++;
    } while (! currentFrequentSubgraphs.isEmpty() &&
      edgeCount <= fsmConfig.getMaxEdgeCount());

    collect(collector, pair.f0,
      allLocallyFrequentSubgraphs, likelyFrequentSubgraphs);
  }

  /**
   * Extracts a collection of subgraphs from a collection of subgraphs with
   * count.
   *
   * @param subgraphsWithCount subgraphs with count
   * @return subgraphs
   */
  private Collection<DFSCode> removeCounts(
    Collection<WithCount<DFSCode>> subgraphsWithCount) {

    Collection<DFSCode> subgraphs =
      Lists.newArrayListWithExpectedSize(subgraphsWithCount.size());

    for (WithCount<DFSCode> subgraphWithCount : subgraphsWithCount) {
      subgraphs.add(subgraphWithCount.getObject());
    }

    return subgraphs;
  }

  /**
   * Counts frequency of subgraphs frequencyed by graphs in a collection.
   *
   * @param graphs graph collection
   * @return a map with Subgraphs and their frequency
   */
  private Map<DFSCode, Integer> countFrequency(Collection<GSpanGraph> graphs) {

    Map<DFSCode, Integer> subgraphFrequency = Maps.newHashMap();

    for (GSpanGraph graph : graphs) {
      if (graph.hasGrownSubgraphs()) {
        for (DFSCode subgraph : graph.getSubgraphEmbeddings().keySet()) {

          Integer frequency = subgraphFrequency.get(subgraph);
          frequency = frequency == null ? 1 : frequency + 1;

          subgraphFrequency.put(subgraph, frequency);
        }
      }
    }

    return subgraphFrequency;
  }

  /**
   * Adds messages about locally and likely frequent subgraphs to collector.
   *
   * @param collector Flink collector
   * @param workerId worker id
   * @param locallyFrequentSubgraphs locally frequent subgraphs
   * @param likelyFrequentSubgraphs likely frequent subgraphs
   */
  private void collect(Collector<FilterMessage> collector, int workerId,
    Collection<WithCount<DFSCode>> locallyFrequentSubgraphs,
    Collection<WithCount<DFSCode>> likelyFrequentSubgraphs) {

    for (WithCount<DFSCode> subgraphWithCount : locallyFrequentSubgraphs) {
      collector.collect(new FilterMessage(
        new CompressedDFSCode(subgraphWithCount.getObject()),
        subgraphWithCount.getCount(), workerId, true));
    }
    for (WithCount<DFSCode> subgraphWithCount : likelyFrequentSubgraphs) {
      collector.collect(new FilterMessage(
        new CompressedDFSCode(subgraphWithCount.getObject()),
        subgraphWithCount.getCount(), workerId, false));
    }
  }
}
