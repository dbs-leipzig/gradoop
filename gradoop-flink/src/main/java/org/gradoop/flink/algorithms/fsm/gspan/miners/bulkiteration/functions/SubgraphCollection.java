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

package org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.pojos.IterationItem;

import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * GROUP REDUCE:
 * (g1, freq1),..,(gN, freqN) => [(g1, freq1),..,(gN, freqN)]
 *
 * MAP:
 * IterationItem[(g1, freq1),..,(gN, freqN)] => (g1, freq1),..,(gN, freqN)
 *
 * REDUCE:
 * [(g1, freq1),..,(gN, freqN)],..,[(g1, freqN+1),..,(gM, freqM)]
 * => [(g1, freq1),..,(gM, freqM)]
 */
public class SubgraphCollection implements GroupReduceFunction
  <WithCount<CompressedDFSCode>, Collection<WithCount<CompressedDFSCode>>>,
  MapFunction<IterationItem, Collection<WithCount<CompressedDFSCode>>>,
  ReduceFunction<Collection<WithCount<CompressedDFSCode>>> {

  /**
   * Frequent Subgraph Mining configuration
   */
  private FSMConfig fsmConfig;

  /**
   * default constructor
   */
  public SubgraphCollection() {
  }

  /**
   * valued constructor
   *
   * @param fsmConfig Frequent Subgraph Mining configuration
   */
  public SubgraphCollection(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public Collection<WithCount<CompressedDFSCode>> map(
    IterationItem collector) throws Exception {

    return collector.getFrequentSubgraphs();
  }

  @Override
  public void reduce(Iterable<WithCount<CompressedDFSCode>> iterable,
    Collector<Collection<WithCount<CompressedDFSCode>>> collector) throws
    Exception {

    List<WithCount<CompressedDFSCode>> subgraphs = Lists.newArrayList();
    Iterator<WithCount<CompressedDFSCode>> iterator = iterable.iterator();

    if (iterator.hasNext()) {
      WithCount<CompressedDFSCode> subgraph = iterator.next();

      if (subgraph.getObject().getDfsCode().size() >=
        fsmConfig.getMinEdgeCount()) {
        subgraphs.add(subgraph);

        while (iterator.hasNext()) {
          subgraphs.add(iterator.next());
        }
      }
    }

    collector.collect(subgraphs);
  }

  @Override
  public Collection<WithCount<CompressedDFSCode>> reduce(
    Collection<WithCount<CompressedDFSCode>> firstSubgraphs,
    Collection<WithCount<CompressedDFSCode>> secondSubgraphs) throws
    Exception {

    Collection<WithCount<CompressedDFSCode>> mergedSubgraphs;

    if (firstSubgraphs.size() >= firstSubgraphs.size()) {
      firstSubgraphs.addAll(secondSubgraphs);
      mergedSubgraphs = firstSubgraphs;
    } else {
      secondSubgraphs.addAll(firstSubgraphs);
      mergedSubgraphs = secondSubgraphs;
    }

    return mergedSubgraphs;
  }
}
